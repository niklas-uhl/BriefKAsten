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
#include "./link_class.hpp"

namespace briefkasten {

/// Memory a queue is willing to have in flight towards it, rationed out as credit. See FlowController.
inline constexpr std::size_t DEFAULT_FLOW_CONTROL_BUDGET_BYTES = 8ULL * 1024 * 1024;

namespace internal {

/// @brief Credit-based flow control: nobody sends payload a peer has not already made room for.
///
/// WHY. Without it, a bounded send backlog is the only backpressure, and a relay that exhausts it blocks
/// inside its receive handler. A persistent receive is re-armed only after its handler returns, so the
/// relay then stops accepting from its row entirely -- measured at 4.09M fully deaf polls per iteration on
/// rmat p=768, against 5.2k on a healthy family. No constant fixes this: every arm of the window sweep,
/// winners included, sat pinned at its cap (128, 512, 1024, 4096, 16384), and the correct constant is both
/// family- and p-dependent. Credits replace the constant with an agreement.
///
/// UNIT: ELEMENTS, not packets. Termination drains send *partial* packets, and the selective drain exists
/// precisely because they do, so a packet is not a fixed quantity of anything and a packet-denominated
/// credit is not conserved. Elements here are buffer elements -- the same unit as buffer.size(),
/// global_buffer_size_ and MessageCounter::pending -- so all four accountings agree.
///
/// THE PROTOCOL, per peer:
///   - the sender tracks `sent` (cumulative elements handed to MPI) against `limit` (cumulative elements
///     the peer has authorised). It may flush a packet of n elements iff n <= limit - sent.
///   - the receiver tracks `consumed` (cumulative elements released) and `granted` (cumulative elements
///     authorised), maintaining granted - consumed <= window.
///   - a grant carries the CUMULATIVE `granted`, never a delta, so a stale or reordered grant is harmless
///     and idempotent -- the receiver of a grant takes the max. At most one grant is in flight per peer; a
///     grant that falls due while one is in flight is coalesced into the next, which is sound precisely
///     because the value is cumulative.
///   - initial windows are IMPLICIT: both ends derive the same base window from the budget and the grid
///     shape, which are identical on every rank, so start-up costs no messages.
///   - grants are never counted by the termination protocol and never reactivate it. Credit returns must
///     bypass the credit system entirely, or the flow-control protocol deadlocks on itself. They travel on
///     their own tag and never touch TerminationCounter.
///
/// RESERVE ON GRANT. This is what makes the receive handler non-blocking, and it is the whole point. A
/// relay grants on a \ref LinkClass::to_proxy link only against forwarding space it has already set aside,
/// so by the time a packet arrives the relay is guaranteed to have room to forward it: the handler only
/// appends, and can never block. It subsumes the separate "local ingestion gate" of the older two-layer
/// sketch. The reserve is enforced structurally rather than by a check -- the base windows of the to_proxy
/// peers plus the shared pool sum to the relay budget, so the total outstanding grant on relay links can
/// never exceed it.
///
/// ALLOCATION. Deliberately NOT an equal split of the send backlog: at p=768 that is 36 packets over 28
/// peers = 1 credit each, which serialises. A configured memory budget is rationed instead, as a per-peer
/// floor plus a shared pool that skewed peers draw from (rmat is exactly the case where a handful of
/// destinations carry the traffic).
///
/// NOT AML'S ACK. AML's acknowledgement is a transport for credit, not a credit: nothing gates on it during
/// the run. Its tag-piggybacked count would force delta-not-cumulative semantics and would invalidate the
/// idempotence argument above, so the two designs cannot be mixed without redoing it.
class FlowController {
public:
    struct Peer {
        /// Which side of the reserve this peer's grants are drawn from. A to_proxy peer's traffic may have
        /// to be forwarded, so its grants are withheld while the relay reserve is full; a to_destination
        /// peer's is consumed on arrival and is never withheld. This is the asymmetry the whole deadlock
        /// argument rests on -- see link_class.hpp.
        LinkClass link_class = LinkClass::to_destination;
        // --- send side ---
        std::size_t sent = 0;   ///< cumulative elements handed to MPI for this peer
        std::size_t limit = 0;  ///< cumulative elements this peer has authorised us to send

        // --- receive side ---
        std::size_t consumed = 0;  ///< cumulative elements from this peer we have released
        std::size_t granted = 0;   ///< cumulative elements we have authorised this peer to send
        std::size_t window = 0;    ///< current window, i.e. the cap on granted - consumed

        // --- grant transport ---
        MPI_Request request = MPI_REQUEST_NULL;
        std::uint64_t outgoing = 0;  ///< the value being sent; must outlive the Isend, hence a member
        bool resend_due = false;     ///< a grant fell due while one was in flight; coalesced into the next
        bool grant_blocked = false;  ///< a grant is due but the relay reserve is full; see maybe_grant
        bool class_known = false;    ///< set once the first packet from this peer fixes its link class
    };

    // NOLINTBEGIN(*-easily-swappable-parameters)
    FlowController(MPI_Comm comm, int tag, std::size_t num_receive_slots)
        : comm_(comm), tag_(tag), receive_requests_(num_receive_slots, MPI_REQUEST_NULL),
          receive_buffers_(num_receive_slots, 0) {}
    // NOLINTEND(*-easily-swappable-parameters)

    FlowController(FlowController const&) = delete;
    FlowController& operator=(FlowController const&) = delete;

    /// Moves leave the source inert: its containers are emptied, so its destructor finds no requests to
    /// cancel and -- crucially -- skips quiesce(), which is collective. A moved-from controller must not
    /// take part in a collective its peers are not in.
    FlowController(FlowController&& other) noexcept
        : comm_(other.comm_), tag_(other.tag_), enabled_(other.enabled_),
          receive_requests_(std::move(other.receive_requests_)),
          receive_buffers_(std::move(other.receive_buffers_)), peers_(std::move(other.peers_)),
          resend_worklist_(std::move(other.resend_worklist_)), base_window_(other.base_window_),
          relay_budget_(other.relay_budget_), proxy_allowance_(other.proxy_allowance_),
          relay_outstanding_(other.relay_outstanding_),
          grant_blocked_(std::move(other.grant_blocked_)), grants_sent_(other.grants_sent_),
          grants_received_(other.grants_received_), num_oversize_passes_(other.num_oversize_passes_),
           num_grants_withheld_(other.num_grants_withheld_) {
        other.enabled_ = false;
        other.receive_requests_.clear();
        other.receive_buffers_.clear();
        other.peers_.clear();
    }

    /// Not implemented on purpose: assigning over a live controller would run quiesce() -- a collective --
    /// at a point its peers know nothing about. Queues are move-constructed into IndirectionAdapter, never
    /// move-assigned.
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

    ///
    /// \p packet_elements is the aggregation threshold in elements. The base window is floored at two
    /// packets so a peer is never reduced to one packet in flight (that is the serialising allocation this
    /// design exists to avoid), and every rank must derive the same numbers from the same inputs -- budget,
    /// grid shape -- because the initial windows are implicit and unexchanged.
    /// Calling it again re-rations without re-arming, which is how IndirectionAdapter narrows a queue
    /// built for a fan-out of p down to the grid's O(sqrt p) peers. Only legal before anything has been
    /// sent, because the initial windows are implicit and a re-ration would silently disagree with a peer
    /// that already holds the old one.
    /// Give every peer a window of \p window_elements and arm the grant channel.
    ///
    /// PER PEER, NOT A RATIONED TOTAL, and that is the whole shape of the memory argument. An absolute
    /// budget divided by the peer count -- which is what this was -- makes the window shrink as p grows
    /// and leaves every derived bound O(1) in p rather than O(peers). Every loose term this class used
    /// to carry came from that one decision:
    ///
    ///   * the per-peer floor fighting the ration, and the budget having to be raised to cover it;
    ///   * a shared pool of unrationed slack, and windows growing into it;
    ///   * relay_high_water needing 2x the budget, because the implicit initial windows summed to a
    ///     budget's worth ON TOP of the gated allowance.
    ///
    /// With a per-peer window all three vanish. The initial windows ARE the allowance rather than an
    /// extra term, so the high-water mark is one window above the gate rather than double the budget,
    /// and everything the caller sizes from it is O(peers).
    ///
    /// Calling it again re-rations, which is how IndirectionAdapter narrows a queue built for a fan-out
    /// of p down to the grid's O(sqrt p) peers. Only legal before anything has been sent, because the
    /// initial windows are implicit and a re-ration would silently disagree with a peer holding the old
    /// one.
    void configure(std::size_t window_elements, std::size_t num_peers) {
        if (window_elements == 0) {
            return;
        }
        KASSERT(peers_.empty(), "flow control must be rationed before the first message");
        base_window_ = window_elements;
        // What every relay peer together may have outstanding toward us. The gate holds
        // relay_outstanding_ + proxy_allowance_ below this, and proxy_allowance_ alone can never exceed
        // it, so the initial windows need no separate term.
        relay_budget_ = window_elements * std::max<std::size_t>(1, num_peers);
        if (!enabled_) {
            enabled_ = true;
            arm_receives();
        }
    }

    [[nodiscard]] bool enabled() const {
        return enabled_;
    }

    [[nodiscard]] std::size_t base_window() const {
        return base_window_;
    }

    /// The most that may be in flight towards this rank: one window per peer.
    [[nodiscard]] std::size_t budget() const {
        return relay_budget_;
    }

    /// The hard bound on relayed payload, and so on the buffers a caller must be able to find for it.
    ///
    /// Two terms only: the gate (\ref maybe_grant stops raising a relay peer's grant once
    /// relay_outstanding_ + proxy_allowance_ reaches relay_budget_), plus the one grant that trips it,
    /// which has already raised a peer's allowance by a window before the check runs.
    ///
    /// relay_budget_ is windows-times-peers, so this is O(peers) -- which is the point. It used to be
    /// 2*budget + max_window because an absolute budget made the implicit initial windows a separate,
    /// uncounted term; with a per-peer window they are the allowance itself.
    [[nodiscard]] std::size_t relay_high_water() const {
        return relay_budget_ + base_window_;
    }

    /// May a packet of \p elements elements go out to \p peer right now?
    ///
    /// The second disjunct is an escape hatch, not a loophole. A single message larger than the whole
    /// window cannot be split by this layer -- aggregation flushes before merging, so an oversize message
    /// ends up alone in its buffer -- and refusing it forever would deadlock. It is let through whenever
    /// any credit at all is left, which over-runs the receiver's reserve by at most one oversize packet per
    /// peer. The reserve is a memory budget, not a hard buffer, so that is bounded and safe; it is counted
    /// (\ref num_oversize_passes) because a workload that hits it often has mis-sized its threshold.
    [[nodiscard]] bool has_credit(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return true;
        }
        Peer& state = peer_state(peer);
        auto const credit = state.limit - std::min(state.limit, state.sent);
        if (elements <= credit) {
            return true;
        }
        if (credit > 0 && elements > state.window) {
            num_oversize_passes_++;
            return true;
        }
        return false;
    }

    void note_sent(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return;
        }
        peer_state(peer).sent += elements;
    }

    /// One arriving packet of \p elements elements from \p peer has been handled. Releases the peer's
    /// window and, if enough of it has come free, grants more.
    ///
    /// Release is immediate even for relayed payload, which looks wrong and is not: the peer's WINDOW is
    /// about fairness between peers, while the memory the relayed payload occupies is bounded separately by
    /// the relay reserve (see \ref note_relayed / \ref note_relay_released). Attributing each forwarded
    /// element back to the incoming peer it came from would need per-buffer, per-source bookkeeping to buy
    /// nothing: the reserve already stops us granting more than we can forward, because the to_proxy base
    /// windows and the pool sum to it.
    void note_admitted(PEID peer, std::size_t elements) {
        if (!enabled_) {
            return;
        }
        Peer& state = peer_state(peer);
        state.consumed += elements;
        if (state.link_class == LinkClass::to_proxy) {
            // This much of the relay allowance has now been spent; it is relay_outstanding_'s problem
            // from here, and stays so until the forwarding send completes.
            proxy_allowance_ -= std::min(proxy_allowance_, elements);
        }
        maybe_grant(peer, state);
    }

    /// Payload admitted over a to_proxy link and merged into an outgoing buffer, i.e. occupying the relay
    /// reserve until its forwarding send completes.
    void note_relayed(std::size_t elements) {
        relay_outstanding_ += elements;
    }

    /// A forwarding send completed (or its payload was discarded by a BufferCleaner): the reserve is free.
    /// Peers whose grants were withheld while it was full get them now -- this is the only thing that
    /// re-opens a relay link, so it must not be skipped.
    void note_relay_released(std::size_t elements) {
        KASSERT(relay_outstanding_ >= elements, "relay reserve accounting underflowed");
        relay_outstanding_ -= std::min(relay_outstanding_, elements);
        if (!grant_blocked_.empty() && relay_outstanding_ + proxy_allowance_ < relay_budget_) {
            auto blocked = std::move(grant_blocked_);
            grant_blocked_.clear();
            for (PEID peer : blocked) {
                Peer& state = peer_state(peer);
                state.grant_blocked = false;
                maybe_grant(peer, state);
            }
        }
    }

    [[nodiscard]] std::size_t relay_outstanding() const {
        return relay_outstanding_;
    }

    /// Receive any grants that have arrived and push out any that fell due while a send was in flight.
    /// Cheap on the common path: one MPI_Testsome plus a walk of the (usually empty) resend worklist.
    void poll() {
        if (!enabled_) {
            return;
        }
        receive_grants();
        progress_resends();
    }

    /// Record which side of the reserve \p peer's grants come from. Idempotent, and called before the
    /// peer's first grant decision.
    void set_link_class(PEID peer, LinkClass cls) {
        if (!enabled_) {
            return;
        }
        Peer& state = peer_state(peer);
        if (state.class_known) {
            KASSERT(state.link_class == cls, "a link's class must not change under us");
            return;
        }
        state.class_known = true;
        state.link_class = cls;
        if (cls == LinkClass::to_proxy) {
            // The implicit initial window was handed out before we knew this was a relay link, so it
            // has to join the allowance now or the first window's worth escapes the bound.
            proxy_allowance_ += state.granted - std::min(state.granted, state.consumed);
        }
    }

    /// Grant decisions deferred because the relay reserve was full, i.e. how often backpressure actually
    /// reached back to a row peer. Zero here with a large num_credit_deferrals means the congestion is on
    /// our own send side, not in relaying.
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

    /// Whole-ledger snapshot for stall tracing. Allocates and formats, so it is only ever called from
    /// BufferedMessageQueue's stall tracer, which is off unless BRIEFKASTEN_STALL_TRACE_SECONDS is set.
    ///
    /// Print two of these a few seconds apart: what has NOT changed between them is the stall. A peer
    /// with credit=0 and no grant arriving is waiting on its peer's consumption; a peer marked
    /// GRANT_BLOCKED is waiting on the relay reserve, which only the completion of a forwarding send can
    /// free.
    [[nodiscard]] std::string describe() const {
        std::ostringstream out;
        out << "fc{enabled=" << enabled_ << " budget=" << relay_budget_
            << " relay_outstanding=" << relay_outstanding_ << " proxy_allowance=" << proxy_allowance_
            << " high_water=" << relay_high_water() << " base_window=" << base_window_
            << " grants_sent=" << grants_sent_
            << " grants_received=" << grants_received_ << " withheld=" << num_grants_withheld_
            << " blocked_list=" << grant_blocked_.size() << " resend_list=" << resend_worklist_.size()
            << " oversize=" << num_oversize_passes_ <<  "}";
        for (auto const& entry : peers_) {
            Peer const& st = entry.second;
            out << "\n    peer " << entry.first
                << (st.link_class == LinkClass::to_proxy ? " PROXY" : " DEST ")
                << " sent=" << st.sent << " limit=" << st.limit
                << " credit=" << (st.limit - std::min(st.limit, st.sent)) << " granted=" << st.granted
                << " consumed=" << st.consumed << " window=" << st.window
                << (st.class_known ? "" : " CLASS_UNKNOWN") << (st.grant_blocked ? " GRANT_BLOCKED" : "")
                << (st.resend_due ? " RESEND_DUE" : "")
                << (st.request != MPI_REQUEST_NULL ? " GRANT_INFLIGHT" : "");
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
        Peer fresh;
        // IMPLICIT INITIAL WINDOW. Both ends run this line with the same base_window_, derived from inputs
        // that are identical on every rank, so no message is needed to agree on it: we may send
        // base_window_ elements before the first grant, and we have implicitly authorised the peer to send
        // us the same. Grants only ever raise these, and carry cumulative values, so an initial value that
        // both sides hold is all the protocol needs to start.
        fresh.window = base_window_;
        fresh.limit = base_window_;
        fresh.granted = base_window_;
        return peers_.emplace(peer, fresh).first->second;
    }


    /// Grant when at least half the window has come free, or immediately when the peer is fully blocked.
    /// The hysteresis is what keeps grant traffic O(peers) per window of payload rather than O(messages):
    /// at the 4 KiB default a window is several packets, so this is far below one grant per packet.
    ///
    /// RESERVE ON GRANT. A relay link's grant is withheld while the relay reserve is full. This is the one
    /// rule that makes the receive handler non-blocking, and it is not optional: without it a peer's
    /// window is refreshed the moment its packet is handled, so it keeps sending while the forwarding
    /// backlog it is feeding grows without bound, the aggregation pool empties, and the relay handler
    /// spins for a buffer -- which is the original defect, reached by a longer road. Withholding cannot
    /// deadlock, because the reserve is freed by forwarding sends, which travel on to_destination links,
    /// whose grants are never withheld (their payload is consumed, not forwarded).
    void maybe_grant(PEID peer, Peer& state) {
        auto const desired = state.consumed + state.window;
        if (desired <= state.granted) {
            return;
        }
        // Both terms, not just the first. relay_outstanding_ is payload we are already holding;
        // proxy_allowance_ is payload our peers are already entitled to send us and which we will have
        // to hold when it arrives. Gating on the sum is what makes relay_high_water() a real bound.
        if (state.link_class == LinkClass::to_proxy &&
            relay_outstanding_ + proxy_allowance_ >= relay_budget_) {
            if (!state.grant_blocked) {
                state.grant_blocked = true;
                grant_blocked_.push_back(peer);
                num_grants_withheld_++;
            }
            return;
        }
        auto const freed = desired - state.granted;
        bool blocked = state.granted <= state.consumed;
        if (!blocked && freed * 2 < state.window) {
            return;
        }
        if (state.link_class == LinkClass::to_proxy) {
            proxy_allowance_ += desired - state.granted;
        }
        state.granted = desired;
        send_grant(peer, state);
        KASSERT(relay_outstanding_ + proxy_allowance_ <= relay_high_water(),
                "relay reserve overshot its high-water mark: outstanding=" << relay_outstanding_
                    << " allowance=" << proxy_allowance_ << " bound=" << relay_high_water());
    }

    void send_grant(PEID peer, Peer& state) {
        if (state.request != MPI_REQUEST_NULL) {
            // At most one grant in flight per peer. Coalescing is free: the value is cumulative, so the
            // next send carries everything this one would have.
            if (!state.resend_due) {
                state.resend_due = true;
                resend_worklist_.push_back(peer);
            }
            return;
        }
        state.outgoing = static_cast<std::uint64_t>(state.granted);
        MPI_Isend(&state.outgoing, 1, MPI_UINT64_T, peer, tag_, comm_, &state.request);
        grants_sent_++;
    }

    void progress_resends() {
        if (resend_worklist_.empty()) {
            return;
        }
        std::size_t out = 0;
        for (std::size_t i = 0; i < resend_worklist_.size(); ++i) {
            PEID peer = resend_worklist_[i];
            Peer& state = peer_state(peer);
            if (state.request != MPI_REQUEST_NULL) {
                int done = 0;
                MPI_Test(&state.request, &done, MPI_STATUS_IGNORE);
                if (done == 0) {
                    resend_worklist_[out++] = peer;  // still in flight, keep it on the list
                    continue;
                }
                state.request = MPI_REQUEST_NULL;
            }
            state.resend_due = false;
            send_grant(peer, state);
        }
        resend_worklist_.resize(out);
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
            // max, not assignment: grants may be reordered relative to each other, and a cumulative counter
            // makes a stale one a no-op rather than a regression.
            state.limit = std::max(state.limit, value);
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

    /// Close the grant channel without leaving unmatched messages behind.
    ///
    /// Grants deliberately do not participate in termination, so when the data protocol agrees to stop there
    /// may still be grants in flight -- and this queue is constructed per phase, so leaking them would
    /// accumulate across a run. Counting them out is the same double-counting argument termination itself
    /// uses, run once on the grant channel: every rank knows how many grants it sent and received, the sums
    /// are global, and no further grant can be issued because no further payload is sent. Every rank
    /// executes the same number of rounds, because the decision comes out of the allreduce.
    void quiesce() {
        std::size_t rounds = 0;
        while (true) {
            // A stall here looks like a hang at the END of a phase, with no other symptom. Cheap to
            // trace and impossible to diagnose otherwise.
            if (++rounds % 1000 == 0 && std::getenv("BRIEFKASTEN_STALL_TRACE_SECONDS") != nullptr) {
                std::fprintf(stderr, "[bk-quiesce] round %zu sent=%zu received=%zu\n", rounds,
                             grants_sent_, grants_received_);
            }
            receive_grants();
            progress_resends();
            std::size_t outstanding = 0;
            for (auto& entry : peers_) {
                Peer& state = entry.second;
                if (state.request != MPI_REQUEST_NULL) {
                    int done = 0;
                    MPI_Test(&state.request, &done, MPI_STATUS_IGNORE);
                    if (done != 0) {
                        state.request = MPI_REQUEST_NULL;
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
    std::vector<PEID> resend_worklist_;
    std::size_t base_window_ = 0;
    std::size_t relay_budget_ = 0;
    /// Payload relay peers are entitled to send us but have not yet sent: sum over to_proxy peers of
    /// (granted - consumed). Held against the same budget as payload we are already carrying.
    std::size_t proxy_allowance_ = 0;
    std::size_t relay_outstanding_ = 0;
    /// Peers whose grant is waiting on the relay reserve; drained by note_relay_released.
    std::vector<PEID> grant_blocked_;
    std::size_t grants_sent_ = 0;
    std::size_t grants_received_ = 0;
    std::size_t num_oversize_passes_ = 0;
    std::size_t num_grants_withheld_ = 0;
};

}  // namespace internal
}  // namespace briefkasten
