// Copyright (c) 2021-2026 Tim Niklas Uhl
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <mpi.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <kassert/kassert.hpp>

#include "./definitions.hpp"

namespace briefkasten::internal {

/// @brief Credit-based flow control: a rank only sends a buffer if the receiving peer has made room for it.
///
/// Directions are always seen from this rank: in/inbound = peer -> this rank, out/outbound = this rank -> peer.
///
/// Per peer, all counts are totals in elements (not buffers, because a proxy re-aggregates what it forwards):
///   - sender:   may send a buffer of n elements iff n <= out_allowed - out_sent.
///   - receiver: keeps in_granted - in_received <= window and sends a grant carrying in_granted
///               whenever at least half a window has freed up.
///
/// Grants carry totals, so a stale or reordered grant is harmless (the sender takes the max), and a grant
/// due while another is in flight is merged into the next one. The initial window is implicit: every rank
/// derives the same value from the config, so start-up needs no messages. Grants use their own tag and are
/// not counted by termination detection.
///
/// Redirection: buffers from a proxied peer (one this rank is the proxy for) may contain messages this rank has to
/// forward. To such peers it only grants while num_redirect_elements_buffered_ + sum_proxied_in_credit_ <
/// max_num_redirect_elements, i.e. it never allows more than it can forward. This is what keeps the receive handler
/// from blocking.
///
/// Credit budget (configure_budget, Config::credit_budget_buffers): instead of a fixed window per link, every link
/// starts at a minimum window of two buffers, and a receiver grows a link's window by one buffer per due grant, up to
/// the maximum window, while the windows of all inbound links (the untouched ones at their initial window included)
/// plus the forwarded elements held stay within the budget. If they already exceed it, the link's window is halved
/// instead (down to the minimum). Credit cannot be revoked, so a link only shrinks when its next grant falls due. The
/// redirect limit becomes the budget, so a proxy holds at most the budget in forwarded elements, and what peers may
/// send this rank is at most the budget. Links to non-proxied peers are never withheld (at most shrunk).
class FlowController {
public:
    struct Peer {
        bool proxied = false;  ///< this rank is the peer's proxy: its buffers may contain messages to forward
        // outbound: this rank -> peer
        std::size_t out_sent = 0;
        std::size_t out_allowed = 0;  ///< from the peer's latest grant
        // inbound: peer -> this rank
        std::size_t in_received = 0;
        std::size_t in_granted = 0;
        // grant transport
        MPI_Request grant_request = MPI_REQUEST_NULL;
        std::uint64_t grant_value = 0;  ///< send buffer of grant_request
        bool grant_pending = false;     ///< a newer grant is waiting for grant_request to complete
        bool grant_withheld = false;    ///< a grant is due, but the redirect limit is reached
        bool proxied_known = false;
        double withheld_since = -1.0;  ///< DIAGNOSTIC: MPI_Wtime() when the current withhold episode began
        std::size_t window = 0;  ///< inbound window of this link (the fixed window without a credit budget)
        // feeder-first: what this proxied peer's buffers carried to forward, and the part of it to destinations
        // with payload parked at that moment (both halved once the total exceeds eight windows, so recent traffic
        // counts most)
        std::size_t forwarded = 0;
        std::size_t forwarded_congested = 0;
    };

    /// Set by configure(), fixed afterwards.
    struct Limits {
        std::size_t num_window_elements = 0;        ///< credit per peer (with a budget: the largest window)
        std::size_t max_num_redirect_elements = 0;  ///< num_window_elements * num_peers, or the budget
        std::size_t initial_window_elements = 0;    ///< implicit first window of every link, identical on both ends
        std::size_t window_step_elements = 0;       ///< growth per due grant (one buffer)
        std::size_t budget_elements = 0;            ///< 0: fixed windows
        std::size_t num_peers = 0;
        /// window of links that are known not to be proxied (only delivered here), with fixed windows; 0: the window
        std::size_t delivery_window_elements = 0;
    };

    // NOLINTBEGIN(*-easily-swappable-parameters)
    FlowController(MPI_Comm comm, int tag, std::size_t num_receive_slots)
        : comm_(comm), tag_(tag), receive_requests_(num_receive_slots, MPI_REQUEST_NULL),
          receive_buffers_(num_receive_slots, 0) {}
    // NOLINTEND(*-easily-swappable-parameters)

    FlowController(FlowController const&) = delete;
    FlowController& operator=(FlowController const&) = delete;

    /// Leaves \p other disabled, so that its destructor does not take part in quiesce(), which is collective.
    FlowController(FlowController&& other) noexcept
        : comm_(other.comm_),
          tag_(other.tag_),
          enabled_(other.enabled_),
          receive_requests_(std::move(other.receive_requests_)),
          receive_buffers_(std::move(other.receive_buffers_)),
          peers_(std::move(other.peers_)),
          pending_grants_(std::move(other.pending_grants_)),
          limits_(other.limits_),
          half_window_pass_(other.half_window_pass_),
          exact_redirect_limit_(other.exact_redirect_limit_),
          sum_proxied_in_credit_(other.sum_proxied_in_credit_),
          num_redirect_elements_buffered_(other.num_redirect_elements_buffered_),
          withheld_grants_(std::move(other.withheld_grants_)),
          grants_sent_(other.grants_sent_),
          grants_received_(other.grants_received_),
          num_oversize_passes_(other.num_oversize_passes_),
          num_grants_withheld_(other.num_grants_withheld_),
          num_withhold_episodes_(other.num_withhold_episodes_),
          withheld_seconds_(other.withheld_seconds_),
          max_withheld_seconds_(other.max_withheld_seconds_),
          sum_touched_in_credit_(other.sum_touched_in_credit_),
          sum_touched_windows_(other.sum_touched_windows_),
          peak_inbound_credit_(other.peak_inbound_credit_),
          num_window_grows_(other.num_window_grows_),
          num_window_shrinks_(other.num_window_shrinks_),
          feeder_first_(other.feeder_first_),
          num_feeder_withholds_(other.num_feeder_withholds_) {
        other.enabled_ = false;
        other.receive_requests_.clear();
        other.receive_buffers_.clear();
        other.peers_.clear();
    }

    FlowController& operator=(FlowController&&) = delete;

    ~FlowController() {
        if (!enabled_) {
            return;
        }
        quiesce();
        for (MPI_Request& request : receive_requests_) {
            if (request != MPI_REQUEST_NULL) {
                MPI_Cancel(&request);
                MPI_Wait(&request, MPI_STATUS_IGNORE);
                MPI_Request_free(&request);
            }
        }
    }

    /// Give every peer a window of \p num_window_elements and arm the grant receives. May be called again to
    /// change the peer count (IndirectionAdapter does), but only before the first message.
    /// EXPERIMENT SWITCH (Config::half_window_oversize_pass): false restores the pass threshold from before
    /// b75b61f, the whole window instead of half of it.
    void half_window_oversize_pass(bool enable) {
        half_window_pass_ = enable;
    }

    void configure(std::size_t num_window_elements, std::size_t num_peers) {
        configure_budget(num_window_elements, num_peers, 0, 0);
    }

    /// With a budget of \p budget_elements (0: fixed windows of \p num_window_elements): links start at two buffers
    /// of \p buffer_elements each (at most the largest window, \p num_window_elements) and grow by one buffer per
    /// due grant while all windows plus the forwarded payload held stay within the budget, which is raised to hold
    /// the initial windows of all \p num_peers peers if necessary.
    void configure_budget(std::size_t num_window_elements, std::size_t num_peers, std::size_t budget_elements,
                          std::size_t buffer_elements) {
        if (num_window_elements == 0) {
            return;
        }
        KASSERT(peers_.empty(), "flow control must be rationed before the first message");
        auto const peers = std::max<std::size_t>(1, num_peers);
        limits_.num_window_elements = num_window_elements;
        limits_.num_peers = peers;
        if (budget_elements == 0) {
            limits_.initial_window_elements = num_window_elements;
            limits_.window_step_elements = 0;
            limits_.budget_elements = 0;
            limits_.max_num_redirect_elements = num_window_elements * peers;
        } else {
            limits_.initial_window_elements = std::min(num_window_elements, 2 * buffer_elements);
            limits_.window_step_elements = std::max<std::size_t>(buffer_elements, 1);
            limits_.budget_elements = std::max(budget_elements, limits_.initial_window_elements * peers);
            limits_.max_num_redirect_elements = limits_.budget_elements;
        }
        peak_inbound_credit_ = inbound_credit();
        if (!enabled_) {
            enabled_ = true;
            arm_receives();
        }
    }

    /// Fixed windows only: links whose buffers this rank only receives (not proxied) get a window of \p elements
    /// instead, once the first buffer has shown that they are not proxied. The redirect limit and the initial window
    /// stay those of the (proxied) window, so the proxy's bound is unchanged: forwarded payload enters only through
    /// proxied links. Larger delivery windows let a proxy pass forwarded payload on faster.
    void delivery_window(std::size_t elements) {
        limits_.delivery_window_elements = elements;
    }

    /// Feeder-first withholding (exact redirect limit only): above three quarters of the redirect limit, a proxy
    /// already withholds due grants from proxied peers whose recent forwarded payload went to at least a quarter to
    /// destinations with payload parked (the feeders of the congestion), and keeps granting the others up to the
    /// limit itself. The limit, and with it the bound, is unchanged; only who waits first changes.
    void feeder_first(bool enable) {
        feeder_first_ = enable;
    }

    /// Feeder-first: \p peer's buffer carried \p elements to forward, \p congested of them to destinations with
    /// payload parked.
    void note_forwarded(PEID peer, std::size_t elements, std::size_t congested) {
        if (!enabled_ || elements == 0) {
            return;
        }
        Peer& state = peer_state(peer);
        state.forwarded += elements;
        state.forwarded_congested += congested;
        if (state.forwarded > 8 * limits_.num_window_elements) {
            state.forwarded /= 2;
            state.forwarded_congested /= 2;
        }
    }

    [[nodiscard]] std::size_t num_feeder_withholds() const {
        return num_feeder_withholds_;
    }

    [[nodiscard]] bool enabled() const {
        return enabled_;
    }

    [[nodiscard]] std::size_t num_window_elements() const {
        return limits_.num_window_elements;
    }

    /// Upper bound on num_redirect_elements_buffered_ + sum_proxied_in_credit_ (apart from oversize passes): the
    /// limit, plus the one grant that reaches it unless the limit is exact.
    [[nodiscard]] std::size_t redirect_high_water() const {
        return limits_.max_num_redirect_elements + (exact_redirect_limit_ ? 0 : limits_.num_window_elements);
    }

    /// EXPERIMENT SWITCH (Config::exact_redirect_limit): a proxy grants on a proxied link only if the whole grant
    /// fits the redirect limit, instead of whenever it is below the limit, so the limit is never overshot.
    void exact_redirect_limit(bool enable) {
        exact_redirect_limit_ = enable;
    }

    /// May a buffer of \p elements elements be sent to \p peer now? A buffer larger than half the window is let
    /// through as soon as any credit is left: the receiver grants only once half the window has freed up, so with
    /// nothing in flight the credit can stay anywhere above half the window, and a larger buffer would wait for a
    /// grant that never comes. Buffers up to the local threshold are at most half the window with two or more credit
    /// buffers; larger ones are large messages (Config::allow_large_messages).
    [[nodiscard]] bool has_credit(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return true;
        }
        Peer& state = peer_state(peer);
        auto const credit = state.out_allowed - std::min(state.out_allowed, state.out_sent);
        if (elements <= credit) {
            return true;
        }
        // counted in track_send: a buffer may be checked again and again while it waits for a send slot
        // the receiver's window of this link is at least the initial one, so half of that is the safe threshold
        auto const pass_above =
            half_window_pass_ ? limits_.initial_window_elements / 2 : limits_.initial_window_elements;
        return credit > 0 && elements > pass_above;
    }

    void track_send(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return;
        }
        Peer& state = peer_state(peer);
        auto const credit = state.out_allowed - std::min(state.out_allowed, state.out_sent);
        if (elements > credit) {  // let through by has_credit as an oversize buffer
            num_oversize_passes_++;
        }
        state.out_sent += elements;
    }

    /// A buffer of \p elements elements from \p peer has been handled; grant more if due.
    void track_receive(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return;
        }
        Peer& state = peer_state(peer);
        auto const outstanding_before = outstanding_credit(state);
        state.in_received += elements;
        sum_touched_in_credit_ -= outstanding_before - outstanding_credit(state);
        if (state.proxied) {  // message from the peer may contain data that we have to forward
            // the granted credit is used up: what we forward of it is now counted as buffered (hold_redirected).
            // Only the credit this peer had outstanding: an oversize pass (has_credit) receives more than that.
            sum_proxied_in_credit_ -= outstanding_before - outstanding_credit(state);
        }
        grant_if_due(peer, state);
    }

    /// Redirected elements were merged into an outgoing buffer and count as buffered until sent.
    void hold_redirected(std::size_t elements) {
        num_redirect_elements_buffered_ += elements;
    }

    /// Redirected elements were sent (or discarded); send the grants withheld while the redirect limit was reached.
    void release_redirected(std::size_t elements) {
        KASSERT(num_redirect_elements_buffered_ >= elements, "redirect accounting underflowed");
        num_redirect_elements_buffered_ -= std::min(num_redirect_elements_buffered_, elements);
        if (!withheld_grants_.empty() &&
            num_redirect_elements_buffered_ + sum_proxied_in_credit_ < limits_.max_num_redirect_elements) {
            auto withheld = std::move(withheld_grants_);
            withheld_grants_.clear();
            for (PEID peer : withheld) {
                Peer& state = peer_state(peer);
                state.grant_withheld = false;
                grant_if_due(peer, state);
            }
        }
    }

    [[nodiscard]] std::size_t num_redirect_elements_buffered() const {
        return num_redirect_elements_buffered_;
    }

    /// Receive grants and send pending ones.
    void poll() {
        if (!enabled_) {
            return;
        }
        receive_grants();
        progress_grants();
    }

    void set_proxied(PEID peer, bool proxied) {
        if (!enabled_) {
            return;
        }
        Peer& state = peer_state(peer);
        if (state.proxied_known) {
            KASSERT(state.proxied == proxied, "proxied must not change for a peer");
            return;
        }
        state.proxied_known = true;
        state.proxied = proxied;
        if (proxied) {
            // the implicit initial window was granted before proxied was known
            sum_proxied_in_credit_ += state.in_granted - std::min(state.in_granted, state.in_received);
        }
    }

    [[nodiscard]] std::size_t num_grants_withheld() const {
        return num_grants_withheld_;
    }

    [[nodiscard]] std::size_t num_oversize_passes() const {
        return num_oversize_passes_;
    }

    /// DIAGNOSTIC: a withhold episode runs from the first withhold of a due grant until the grant is sent;
    /// num_grants_withheld() also counts the retries in between.
    [[nodiscard]] std::size_t num_withhold_episodes() const {
        return num_withhold_episodes_;
    }

    /// DIAGNOSTIC: summed over the episodes that ended (link-seconds a proxied peer waited for a due grant).
    [[nodiscard]] double withheld_seconds() const {
        return withheld_seconds_;
    }

    [[nodiscard]] double max_withheld_seconds() const {
        return max_withheld_seconds_;
    }

    /// DIAGNOSTIC: the peers whose withhold episode began since the last call.
    std::vector<PEID> take_new_episode_peers() {
        std::vector<PEID> peers;
        peers.swap(new_episode_peers_);
        return peers;
    }

    /// DIAGNOSTIC: credit granted to proxied peers and not yet used, in elements.
    [[nodiscard]] std::size_t sum_proxied_in_credit() const {
        return sum_proxied_in_credit_;
    }

    [[nodiscard]] std::size_t max_num_redirect_elements() const {
        return limits_.max_num_redirect_elements;
    }

    [[nodiscard]] std::size_t num_grants_sent() const {
        return grants_sent_;
    }

    [[nodiscard]] std::size_t num_grants_received() const {
        return grants_received_;
    }

    /// The most credit outstanding on all inbound links at once (untouched links at their initial window included):
    /// what peers could have sent this rank without waiting, i.e. a bound on its unexpected messages.
    [[nodiscard]] std::size_t peak_inbound_credit() const {
        return peak_inbound_credit_;
    }

    /// Credit budget: due grants that grew their link's window, and ones that halved it because the budget was full.
    [[nodiscard]] std::size_t num_window_grows() const {
        return num_window_grows_;
    }

    [[nodiscard]] std::size_t num_window_shrinks() const {
        return num_window_shrinks_;
    }

    [[nodiscard]] std::size_t budget_elements() const {
        return limits_.budget_elements;
    }

    /// Snapshot of all counters, for BufferedMessageQueue's stall tracer.
    [[nodiscard]] std::string describe() const {
        std::ostringstream out;
        out << "fc{enabled=" << enabled_ << " max_redirect=" << limits_.max_num_redirect_elements
            << " redirect_buffered=" << num_redirect_elements_buffered_
            << " sum_proxied_in_credit=" << sum_proxied_in_credit_ << " high_water=" << redirect_high_water()
            << " window=" << limits_.num_window_elements << " grants_sent=" << grants_sent_
            << " grants_received=" << grants_received_ << " withheld=" << num_grants_withheld_
            << " withheld_list=" << withheld_grants_.size() << " pending_list=" << pending_grants_.size()
            << " oversize=" << num_oversize_passes_ << "}";
        for (auto const& entry : peers_) {
            Peer const& st = entry.second;
            out << "\n    peer " << entry.first << (st.proxied ? " PROXIED" : " DEST ") << " out_sent=" << st.out_sent
                << " out_allowed=" << st.out_allowed
                << " credit=" << (st.out_allowed - std::min(st.out_allowed, st.out_sent))
                << " in_granted=" << st.in_granted << " in_received=" << st.in_received
                << (st.proxied_known ? "" : " PROXIED_UNKNOWN") << (st.grant_withheld ? " GRANT_WITHHELD" : "")
                << (st.grant_pending ? " GRANT_PENDING" : "")
                << (st.grant_request != MPI_REQUEST_NULL ? " GRANT_INFLIGHT" : "");
        }
        return out.str();
    }

    void reset_counters() {
        num_oversize_passes_ = 0;
        num_grants_withheld_ = 0;
        num_withhold_episodes_ = 0;
        withheld_seconds_ = 0.0;
        max_withheld_seconds_ = 0.0;
        peak_inbound_credit_ = inbound_credit();
        num_window_grows_ = 0;
        num_window_shrinks_ = 0;
        num_feeder_withholds_ = 0;
    }

private:
    Peer& peer_state(PEID peer) {
        auto it = peers_.find(peer);
        if (it != peers_.end()) {
            return it->second;
        }
        // implicit initial window, identical on both ends
        Peer fresh;
        fresh.out_allowed = limits_.initial_window_elements;
        fresh.in_granted = limits_.initial_window_elements;
        fresh.window = limits_.initial_window_elements;
        sum_touched_in_credit_ += fresh.in_granted;
        sum_touched_windows_ += fresh.window;
        return peers_.emplace(peer, fresh).first->second;
    }

    /// Credit outstanding on all inbound links: the touched ones as tracked, every other expected peer at its
    /// implicit initial window.
    [[nodiscard]] std::size_t inbound_credit() const {
        auto const untouched = limits_.num_peers - std::min(limits_.num_peers, peers_.size());
        return sum_touched_in_credit_ + (untouched * limits_.initial_window_elements);
    }

    /// The windows of all inbound links (every expected peer not seen yet at its initial window). A link's credit
    /// never exceeds its window, and a grant at an unchanged window is never refused on a non-proxied link, so this
    /// is what the budget has to cover.
    [[nodiscard]] std::size_t inbound_windows() const {
        auto const untouched = limits_.num_peers - std::min(limits_.num_peers, peers_.size());
        return sum_touched_windows_ + (untouched * limits_.initial_window_elements);
    }

    /// The credit granted to \p state's peer and not yet used: in_granted - in_received, 0 after an oversize pass.
    [[nodiscard]] static std::size_t outstanding_credit(Peer const& state) {
        return state.in_granted - std::min(state.in_granted, state.in_received);
    }

    /// Grant once at least half the window has freed up. On a proxied link, withhold the grant while the
    /// redirect limit is reached; release_redirected() retries it.
    void grant_if_due(PEID peer, Peer& state) {
        auto const window = state.window;
        if (state.in_received + window <= state.in_granted) {
            return;
        }
        auto const freed = state.in_received + window - state.in_granted;
        // the new outstanding credit is the whole window: after an oversize pass in_received exceeds the old
        // grant, and that excess was never credit
        auto const added_for = [&](std::size_t w) {
            auto const granted = state.in_received + w;
            auto const base = std::max(state.in_granted, state.in_received);
            return granted > base ? granted - base : 0;
        };
        auto const withhold = [&] {
            if (!state.grant_withheld) {
                state.grant_withheld = true;
                withheld_grants_.push_back(peer);
                num_grants_withheld_++;
            }
            if (state.withheld_since < 0.0) {
                state.withheld_since = MPI_Wtime();
                num_withhold_episodes_++;
                new_episode_peers_.push_back(peer);
            }
        };
        auto next_window = window;
        if (limits_.budget_elements == 0 && limits_.delivery_window_elements > 0 && state.proxied_known &&
            !state.proxied) {
            next_window = limits_.delivery_window_elements;
        }
        if (limits_.budget_elements > 0 && freed * 2 >= window) {
            // every window is reserved in full (a link is refilled to it without asking the budget), so growth must
            // fit the budget next to all windows and the forwarded payload held; if they already exceed it, halve
            auto const committed = inbound_windows() + num_redirect_elements_buffered_;
            auto const grown = std::min(window + limits_.window_step_elements, limits_.num_window_elements);
            if (committed + (grown - window) <= limits_.budget_elements) {
                next_window = grown;
            } else if (committed > limits_.budget_elements) {
                next_window = std::max(window / 2, limits_.initial_window_elements);
            }
            if (state.in_received + next_window <= state.in_granted) {
                // more than the smaller window is still outstanding: keep the window (and its reservation) until
                // enough has arrived for a grant at the smaller one
                return;
            }
        }
        auto const added = added_for(next_window);
        auto const held = num_redirect_elements_buffered_ + sum_proxied_in_credit_;
        if (exact_redirect_limit_) {
            // only a grant that is due can be withheld, and it is withheld unless all of it fits the limit
            if (freed * 2 < window) {
                return;
            }
            if (state.proxied && held + added > limits_.max_num_redirect_elements) {
                withhold();
                return;
            }
            if (feeder_first_ && state.proxied && 4 * (held + added) > 3 * limits_.max_num_redirect_elements &&
                4 * state.forwarded_congested >= state.forwarded && state.forwarded > 0) {
                num_feeder_withholds_++;
                withhold();
                return;
            }
        } else {
            if (state.proxied && held >= limits_.max_num_redirect_elements) {
                withhold();
                return;
            }
            if (freed * 2 < window) {
                return;
            }
        }
        if (next_window > window) {
            num_window_grows_++;
        } else if (next_window < window) {
            num_window_shrinks_++;
        }
        sum_touched_windows_ += next_window;
        sum_touched_windows_ -= window;
        state.window = next_window;
        if (state.proxied) {
            sum_proxied_in_credit_ += added;
        }
        sum_touched_in_credit_ += added;
        if (state.withheld_since >= 0.0) {
            auto const waited = MPI_Wtime() - state.withheld_since;
            withheld_seconds_ += waited;
            max_withheld_seconds_ = std::max(max_withheld_seconds_, waited);
            state.withheld_since = -1.0;
        }
        state.in_granted = state.in_received + next_window;
        peak_inbound_credit_ = std::max(peak_inbound_credit_, inbound_credit());
        send_grant(peer, state);
        // Only a grant on a proxied link adds to what this rank may have to forward. A grant to any other peer is
        // unrelated to the redirect limit; the limit can be exceeded while an oversize buffer (has_credit) is being
        // forwarded, and that must not hold back grants to peers that send nothing to forward.
        KASSERT(!state.proxied ||
                    num_redirect_elements_buffered_ + sum_proxied_in_credit_ <= redirect_high_water(),
                "redirect elements overshot their high-water mark: buffered=" << num_redirect_elements_buffered_
                                                                              << " credit=" << sum_proxied_in_credit_
                                                                              << " bound=" << redirect_high_water());
    }

    /// At most one grant per peer is in flight; a newer one waits in pending_grants_.
    void send_grant(PEID peer, Peer& state) {
        if (state.grant_request != MPI_REQUEST_NULL) {
            if (!state.grant_pending) {
                state.grant_pending = true;
                pending_grants_.push_back(peer);
            }
            return;
        }
        state.grant_value = static_cast<std::uint64_t>(state.in_granted);
        MPI_Isend(&state.grant_value, 1, MPI_UINT64_T, peer, tag_, comm_, &state.grant_request);
        grants_sent_++;
    }

    void progress_grants() {
        if (pending_grants_.empty()) {
            return;
        }
        std::size_t out = 0;
        for (std::size_t i = 0; i < pending_grants_.size(); ++i) {
            PEID peer = pending_grants_[i];
            Peer& state = peer_state(peer);
            if (state.grant_request != MPI_REQUEST_NULL) {
                int done = 0;
                MPI_Test(&state.grant_request, &done, MPI_STATUS_IGNORE);
                if (done == 0) {
                    pending_grants_[out++] = peer;
                    continue;
                }
                state.grant_request = MPI_REQUEST_NULL;
            }
            state.grant_pending = false;
            send_grant(peer, state);
        }
        pending_grants_.resize(out);
    }

    void receive_grants() {
        if (receive_requests_.empty()) {
            return;
        }
        int num_completed = 0;
        indices_.resize(receive_requests_.size());
        statuses_.resize(receive_requests_.size());
        MPI_Testsome(static_cast<int>(receive_requests_.size()), receive_requests_.data(), &num_completed,
                     indices_.data(), statuses_.data());
        if (num_completed == 0 || num_completed == MPI_UNDEFINED) {
            return;
        }
        for (int i = 0; i < num_completed; ++i) {
            int const slot = indices_[static_cast<std::size_t>(i)];
            PEID source = statuses_[static_cast<std::size_t>(i)].MPI_SOURCE;
            auto const value = static_cast<std::size_t>(receive_buffers_[static_cast<std::size_t>(slot)]);
            Peer& state = peer_state(source);
            // max: grants may arrive out of order
            state.out_allowed = std::max(state.out_allowed, value);
            grants_received_++;
            MPI_Start(&receive_requests_[static_cast<std::size_t>(slot)]);
        }
    }

    void arm_receives() {
        for (std::size_t i = 0; i < receive_requests_.size(); ++i) {
            MPI_Recv_init(&receive_buffers_[i], 1, MPI_UINT64_T, MPI_ANY_SOURCE, tag_, comm_,
                          &receive_requests_[i]);
            MPI_Start(&receive_requests_[i]);
        }
    }

    /// Wait until every grant sent has been received, so none is left unmatched when the queue is destroyed.
    /// Grants are not part of termination detection, so this runs its own sent == received allreduce.
    void quiesce() {
        std::size_t rounds = 0;
        while (true) {
#ifdef BRIEFKASTEN_STALL_TRACE
            if (++rounds % 1000 == 0 && std::getenv("BRIEFKASTEN_STALL_TRACE_SECONDS") != nullptr) {
                std::fprintf(stderr, "[bk-quiesce] round %zu sent=%zu received=%zu\n", rounds,
                             grants_sent_, grants_received_);
            }
#else
            (void)rounds;
#endif
            receive_grants();
            progress_grants();
            std::size_t outstanding = 0;
            for (auto& entry : peers_) {
                Peer& state = entry.second;
                if (state.grant_request != MPI_REQUEST_NULL) {
                    int done = 0;
                    MPI_Test(&state.grant_request, &done, MPI_STATUS_IGNORE);
                    if (done != 0) {
                        state.grant_request = MPI_REQUEST_NULL;
                    } else {
                        outstanding++;
                    }
                }
            }
            std::size_t local[3] = {grants_sent_, grants_received_, outstanding};
            std::size_t global[3] = {0, 0, 0};
            MPI_Allreduce(static_cast<void*>(local), static_cast<void*>(global), 3, MPI_UINT64_T, MPI_SUM,
                          comm_);
            if (global[0] == global[1] && global[2] == 0) {
                return;
            }
        }
    }

    MPI_Comm comm_;
    int tag_;
    bool enabled_ = false;
    std::vector<MPI_Request> receive_requests_;
    std::vector<std::uint64_t> receive_buffers_;
    std::vector<int> indices_;
    std::vector<MPI_Status> statuses_;
    std::unordered_map<PEID, Peer> peers_;
    std::vector<PEID> pending_grants_;
    Limits limits_;
    bool half_window_pass_ = true;        ///< see half_window_oversize_pass
    bool exact_redirect_limit_ = false;  ///< see exact_redirect_limit
    /// sum of in_granted - in_received over proxied peers: what they may still send us, in elements
    std::size_t sum_proxied_in_credit_ = 0;
    /// received from proxied peers, merged for forwarding, not yet sent
    std::size_t num_redirect_elements_buffered_ = 0;
    std::vector<PEID> withheld_grants_;
    std::size_t grants_sent_ = 0;
    std::size_t grants_received_ = 0;
    std::size_t num_oversize_passes_ = 0;
    std::size_t num_grants_withheld_ = 0;
    std::size_t num_withhold_episodes_ = 0;
    double withheld_seconds_ = 0.0;
    double max_withheld_seconds_ = 0.0;
    std::vector<PEID> new_episode_peers_;
    /// sum of in_granted - in_received over the peers in peers_ (untouched peers: see inbound_credit)
    std::size_t sum_touched_in_credit_ = 0;
    std::size_t sum_touched_windows_ = 0;  ///< sum of the windows of the peers in peers_
    std::size_t peak_inbound_credit_ = 0;
    std::size_t num_window_grows_ = 0;
    std::size_t num_window_shrinks_ = 0;
    bool feeder_first_ = false;
    std::size_t num_feeder_withholds_ = 0;  ///< withholds below the limit, of feeders (feeder_first)
};

}  // namespace briefkasten::internal
