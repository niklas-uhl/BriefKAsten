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
/// Per peer, all counts are totals in elements (not buffers, because a proxy re-aggregates what it forwards):
///   - sender:   may send a buffer of n elements iff n <= total_allowed - total_sent.
///   - receiver: keeps total_granted - total_received <= window and sends a grant carrying total_granted
///               whenever at least half a window has freed up.
///
/// Grants carry totals, so a stale or reordered grant is harmless (the sender takes the max), and a grant
/// due while another is in flight is merged into the next one. The initial window is implicit: every rank
/// derives the same value from the config, so start-up needs no messages. Grants use their own tag and are
/// not counted by termination detection.
///
/// Redirection: buffers from a may_redirect peer may contain messages this rank has to forward. To such peers it
/// only grants while num_redirect_elements_buffered_ + num_redirect_elements_granted_ < max_num_redirect_elements,
/// i.e. it never allows more than it can forward. This is what keeps the receive handler from blocking.
class FlowController {
public:
    struct Peer {
        bool may_redirect = false;  ///< buffers from this peer may contain messages we have to forward
        // send side
        std::size_t total_sent = 0;
        std::size_t total_allowed = 0;  ///< from the peer's latest grant
        // receive side
        std::size_t total_received = 0;
        std::size_t total_granted = 0;
        // grant transport
        MPI_Request grant_request = MPI_REQUEST_NULL;
        std::uint64_t grant_value = 0;  ///< send buffer of grant_request
        bool grant_pending = false;     ///< a newer grant is waiting for grant_request to complete
        bool grant_withheld = false;    ///< a grant is due, but the redirect limit is reached
        bool may_redirect_known = false;
    };

    /// Set by configure(), fixed afterwards.
    struct Limits {
        std::size_t num_window_elements = 0;        ///< credit per peer
        std::size_t max_num_redirect_elements = 0;  ///< num_window_elements * num_peers
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
          num_redirect_elements_granted_(other.num_redirect_elements_granted_),
          num_redirect_elements_buffered_(other.num_redirect_elements_buffered_),
          withheld_grants_(std::move(other.withheld_grants_)),
          grants_sent_(other.grants_sent_),
          grants_received_(other.grants_received_),
          num_oversize_passes_(other.num_oversize_passes_),
          num_grants_withheld_(other.num_grants_withheld_) {
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
    void configure(std::size_t num_window_elements, std::size_t num_peers) {
        if (num_window_elements == 0) {
            return;
        }
        KASSERT(peers_.empty(), "flow control must be rationed before the first message");
        limits_.num_window_elements = num_window_elements;
        limits_.max_num_redirect_elements = num_window_elements * std::max<std::size_t>(1, num_peers);
        if (!enabled_) {
            enabled_ = true;
            arm_receives();
        }
    }

    [[nodiscard]] bool enabled() const {
        return enabled_;
    }

    [[nodiscard]] std::size_t num_window_elements() const {
        return limits_.num_window_elements;
    }

    /// Upper bound on num_redirect_elements_buffered_ + num_redirect_elements_granted_: the limit, plus the one grant
    /// that reaches it.
    [[nodiscard]] std::size_t redirect_high_water() const {
        return limits_.max_num_redirect_elements + limits_.num_window_elements;
    }

    /// May a buffer of \p elements elements be sent to \p peer now? A buffer larger than the whole window
    /// is let through as soon as any credit is left, since it could otherwise never be sent.
    [[nodiscard]] bool has_credit(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return true;
        }
        Peer& state = peer_state(peer);
        auto const credit = state.total_allowed - std::min(state.total_allowed, state.total_sent);
        if (elements <= credit) {
            return true;
        }
        if (credit > 0 && elements > limits_.num_window_elements) {  // does not happen in normal operation
            num_oversize_passes_++;
            return true;
        }
        return false;
    }

    void track_send(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return;
        }
        peer_state(peer).total_sent += elements;
    }

    /// A buffer of \p elements elements from \p peer has been handled; grant more if due.
    void track_receive(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return;
        }
        Peer& state = peer_state(peer);
        state.total_received += elements;
        if (state.may_redirect) {  // message from the peer may contain data that we have to forward
            // the granted credit is used up: what we forward of it is now counted as buffered (hold_redirected)
            num_redirect_elements_granted_ -= std::min(num_redirect_elements_granted_, elements);
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
            num_redirect_elements_buffered_ + num_redirect_elements_granted_ < limits_.max_num_redirect_elements) {
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

    void set_may_redirect(PEID peer, bool may_redirect) {
        if (!enabled_) {
            return;
        }
        Peer& state = peer_state(peer);
        if (state.may_redirect_known) {
            KASSERT(state.may_redirect == may_redirect, "may_redirect must not change for a peer");
            return;
        }
        state.may_redirect_known = true;
        state.may_redirect = may_redirect;
        if (may_redirect) {
            // the implicit initial window was granted before may_redirect was known
            num_redirect_elements_granted_ += state.total_granted - std::min(state.total_granted, state.total_received);
        }
    }

    [[nodiscard]] std::size_t num_grants_withheld() const {
        return num_grants_withheld_;
    }

    [[nodiscard]] std::size_t num_oversize_passes() const {
        return num_oversize_passes_;
    }

    [[nodiscard]] std::size_t num_grants_sent() const {
        return grants_sent_;
    }

    [[nodiscard]] std::size_t num_grants_received() const {
        return grants_received_;
    }

    /// Snapshot of all counters, for BufferedMessageQueue's stall tracer.
    [[nodiscard]] std::string describe() const {
        std::ostringstream out;
        out << "fc{enabled=" << enabled_ << " max_redirect=" << limits_.max_num_redirect_elements
            << " redirect_buffered=" << num_redirect_elements_buffered_
            << " redirect_granted=" << num_redirect_elements_granted_ << " high_water=" << redirect_high_water()
            << " window=" << limits_.num_window_elements << " grants_sent=" << grants_sent_
            << " grants_received=" << grants_received_ << " withheld=" << num_grants_withheld_
            << " withheld_list=" << withheld_grants_.size() << " pending_list=" << pending_grants_.size()
            << " oversize=" << num_oversize_passes_ << "}";
        for (auto const& entry : peers_) {
            Peer const& st = entry.second;
            out << "\n    peer " << entry.first
                << (st.may_redirect ? " REDIRECT" : " DEST ")
                << " total_sent=" << st.total_sent << " total_allowed=" << st.total_allowed
                << " credit=" << (st.total_allowed - std::min(st.total_allowed, st.total_sent))
                << " total_granted=" << st.total_granted << " total_received=" << st.total_received
                << (st.may_redirect_known ? "" : " MAY_REDIRECT_UNKNOWN") << (st.grant_withheld ? " GRANT_WITHHELD" : "")
                << (st.grant_pending ? " GRANT_PENDING" : "")
                << (st.grant_request != MPI_REQUEST_NULL ? " GRANT_INFLIGHT" : "");
        }
        return out.str();
    }

    void reset_counters() {
        num_oversize_passes_ = 0;
        num_grants_withheld_ = 0;
    }

private:
    Peer& peer_state(PEID peer) {
        auto it = peers_.find(peer);
        if (it != peers_.end()) {
            return it->second;
        }
        // implicit initial window, identical on both ends
        Peer fresh;
        fresh.total_allowed = limits_.num_window_elements;
        fresh.total_granted = limits_.num_window_elements;
        return peers_.emplace(peer, fresh).first->second;
    }

    /// Grant once at least half the window has freed up. On a may_redirect link, withhold the grant while the
    /// redirect limit is reached; release_redirected() retries it.
    void grant_if_due(PEID peer, Peer& state) {
        auto const new_total_granted = state.total_received + limits_.num_window_elements;
        if (new_total_granted <= state.total_granted) {
            return;
        }
        if (state.may_redirect &&
            num_redirect_elements_buffered_ + num_redirect_elements_granted_ >= limits_.max_num_redirect_elements) {
            if (!state.grant_withheld) {
                state.grant_withheld = true;
                withheld_grants_.push_back(peer);
                num_grants_withheld_++;
            }
            return;
        }
        auto const freed = new_total_granted - state.total_granted;
        if (freed * 2 < limits_.num_window_elements) {
            return;
        }
        if (state.may_redirect) {
            num_redirect_elements_granted_ += new_total_granted - state.total_granted;
        }
        state.total_granted = new_total_granted;
        send_grant(peer, state);
        KASSERT(num_redirect_elements_buffered_ + num_redirect_elements_granted_ <= redirect_high_water(),
                "redirect elements overshot their high-water mark: buffered="
                    << num_redirect_elements_buffered_ << " granted=" << num_redirect_elements_granted_
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
        state.grant_value = static_cast<std::uint64_t>(state.total_granted);
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
            state.total_allowed = std::max(state.total_allowed, value);
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
    /// granted to may_redirect peers but not yet received
    std::size_t num_redirect_elements_granted_ = 0;
    /// received from may_redirect peers, merged for forwarding, not yet sent
    std::size_t num_redirect_elements_buffered_ = 0;
    std::vector<PEID> withheld_grants_;
    std::size_t grants_sent_ = 0;
    std::size_t grants_received_ = 0;
    std::size_t num_oversize_passes_ = 0;
    std::size_t num_grants_withheld_ = 0;
};

}  // namespace briefkasten::internal
