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
#include <kamping/communicator.hpp>
#include <kassert/kassert.hpp>
#include "./buffered_queue.hpp"   // IWYU pragma: keep
#include "./detail/concepts.hpp"  // IWYU pragma: keep
#include "./detail/definitions.hpp"

#include <kamping/measurements/timer.hpp>

namespace briefkasten {
template <typename T>
concept IndirectionScheme = requires(T scheme, MPI_Comm comm, PEID sender, PEID receiver) {
    { scheme.next_hop(sender, receiver) } -> std::same_as<PEID>;
    { scheme.should_redirect(sender, receiver) } -> std::same_as<bool>;
    // The grouping the scheme routes over: messages within a group go directly, cross-group messages go via one proxy
    // per group. `group_size` is the number of ranks per group, `num_groups` the number of groups. Together they bound
    // each hop's fan-out and drive the buffering defaults (see first_hop_fan_out / second_hop_fan_out below).
    { scheme.group_size() } -> std::convertible_to<std::size_t>;
    { scheme.num_groups() } -> std::convertible_to<std::size_t>;
};

template <IndirectionScheme Indirector, typename BufferedQueueType>
class IndirectionAdapter {
private:
    using queue_type = BufferedQueueType;
    using MessageType = typename queue_type::message_type;

    kamping::Communicator<> first_hop_queue_comm_;
    queue_type first_hop_queue_;
    kamping::Communicator<> second_hop_queue_comm_;
    queue_type second_hop_queue_;

public:
    IndirectionAdapter(BufferedQueueType queue, Indirector indirector)
        : first_hop_queue_comm_(queue.communicator(), false),
          first_hop_queue_(std::move(queue)),
          second_hop_queue_comm_(first_hop_queue_comm_),
          second_hop_queue_(second_hop_queue_comm_.mpi_communicator(),
                            derive_indirection_config(first_hop_queue_.config(), second_hop_fan_out(indirector))),
          indirection_(std::move(indirector)) {
        // Size each hop to its own fan-out (the first hop is larger: it also carries intra-group direct traffic). The
        // first-hop queue was built and moved in by the caller, so its send backlog is already baked into its sender;
        // reconfigure it here to the first-hop defaults.
        auto first_cfg = derive_indirection_config(first_hop_queue_.config(), first_hop_fan_out(indirection_));
        first_hop_queue_.max_num_aggregation_buffers(first_cfg.max_num_aggregation_buffers.value());
        first_hop_queue_.send_backlog_capacity(first_cfg.send_backlog_capacity.value());
    }

    /// Whether the relay keeps polling the first hop while blocked on second-hop send capacity. Default true
    /// preserves the historical behaviour; false is the AML-style rule (only a loop blocked on the first hop may
    /// drive the first hop). See post_message_blocking's direct_send branch.
    void relay_drains_first_hop(bool enable) {
        relay_drains_first_hop_ = enable;
    }

    [[nodiscard]] bool relay_drains_first_hop() const {
        return relay_drains_first_hop_;
    }

    auto& indirection_scheme() {
        return indirection_;
    }

    auto const& indirection_scheme() const {
        return indirection_;
    }

    bool post_message(InputMessageRange<MessageType> auto&& message,
                      PEID receiver,  // NOLINT(bugprone-*)
                      PEID envelope_sender,
                      PEID envelope_receiver,
                      int tag,
                      bool direct_send = false) {
        PEID next_hop = receiver;
        if (direct_send) {
            return second_hop_queue_.post_message(std::forward<decltype(message)>(message), next_hop, envelope_sender,
                                                  envelope_receiver, tag);
        }
        next_hop = indirection_.next_hop(envelope_sender, envelope_receiver);
        return first_hop_queue_.post_message(std::forward<decltype(message)>(message), next_hop, envelope_sender,
                                             envelope_receiver, tag);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(InputMessageRange<MessageType> auto&& message,
                      PEID receiver,
                      int tag = 0,
                      bool direct_send = false) {
        return post_message(std::forward<decltype(message)>(message), receiver, this->rank(), receiver, tag,
                            direct_send);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(typename queue_type::message_type message, PEID receiver, int tag = 0, bool direct_send = false) {
        return post_message(std::ranges::views::single(message), receiver, tag, direct_send);
    }

    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,  // NOLINT(bugprone-*)
                               PEID envelope_sender,
                               PEID envelope_receiver,
                               int tag,
                               MessageHandler<MessageType> auto&& on_message,
                               bool direct_send = false) {
        PEID next_hop = receiver;
        if (direct_send) {
            // This is the relay path: we are inside a first-hop receive handler, forwarding to the final destination.
            //
            // Historically we kept draining the first-hop queue while blocked here, on the reasoning that peers
            // blocked on the first hop would otherwise never receive (and thus complete) the sends we wait on. That
            // hook is also what removes the system's only backpressure path: a rank starved of second-hop capacity
            // keeps ingesting first-hop work it demonstrably cannot forward, so its senders never see the stall and
            // never stop producing. Dropping it makes the eight persistent receives fill, which blocks our first-hop
            // peers in their own post_message_blocking and propagates back to the originators for free.
            //
            // Safe to drop only because every blocking loop now drives the second hop: the spin inside
            // resolve_overflow_blocking polls *this* queue, and flush_all_buffers_blocking finally takes a progress
            // hook (see buffered_queue.hpp). AML enforces exactly this rule -- its flush_buffer_intra spins on
            // aml_poll_intra alone -- and does not deadlock. Kept behind a flag so the two can be measured against
            // each other; see notes/takeover_relay_backpressure.md.
            if (relay_drains_first_hop_) {
                return second_hop_queue_.post_message_blocking(
                    std::forward<decltype(message)>(message), next_hop, envelope_sender, envelope_receiver, tag,
                    on_message, [&] { first_hop_queue_.poll(redirection_handler(on_message)); });
            }
            return second_hop_queue_.post_message_blocking(std::forward<decltype(message)>(message), next_hop,
                                                           envelope_sender, envelope_receiver, tag, on_message,
                                                           [] {});
        }
        next_hop = indirection_.next_hop(envelope_sender, envelope_receiver);
        // Symmetrically, while blocked on the first hop keep draining the second hop so final messages get received.
        return first_hop_queue_.post_message_blocking(
            std::forward<decltype(message)>(message), next_hop, envelope_sender, envelope_receiver, tag,
            redirection_handler(on_message), [&] { second_hop_queue_.poll(on_message); });
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0,
                               bool direct_send = false) {
        return post_message_blocking(std::forward<decltype(message)>(message), receiver, this->rank(), receiver, tag,
                                     std::forward<decltype(on_message)>(on_message), direct_send);
    }

    bool post_message_blocking(MessageType message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0,
                               bool direct_send = false) {
        KASSERT(receiver < this->size());
        return post_message_blocking(std::ranges::views::single(message), receiver,
                                     std::forward<decltype(on_message)>(on_message), tag, direct_send);
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    auto poll(MessageHandler<typename queue_type::message_type> auto&& on_message)
        -> std::optional<std::pair<bool, bool>> {
        std::optional<std::pair<bool, bool>> first_result =
            first_hop_queue_.poll(redirection_handler(std::forward<decltype(on_message)>(on_message)));
        std::optional<std::pair<bool, bool>> second_result =
            second_hop_queue_.poll(std::forward<decltype(on_message)>(on_message));
        if (!first_result) {
            return second_result;
        }
        if (!second_result) {
            return first_result;
        }
        return {std::pair{first_result->first || second_result->first, first_result->second || second_result->second}};
    }

    auto poll_throttled(MessageHandler<MessageType> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD)
        -> std::optional<std::pair<bool, bool>> {
        std::optional<std::pair<bool, bool>> first_result = first_hop_queue_.poll_throttled(
            redirection_handler(std::forward<decltype(on_message)>(on_message)), poll_skip_threshold);
        std::optional<std::pair<bool, bool>> second_result =
            second_hop_queue_.poll_throttled(std::forward<decltype(on_message)>(on_message), poll_skip_threshold);
        if (!first_result) {
            return second_result;
        }
        if (!second_result) {
            return first_result;
        }
        return {std::pair{first_result->first || second_result->first, first_result->second || second_result->second}};
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return terminate(std::forward<decltype(on_message)>(on_message), []() {});
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<typename queue_type::message_type> auto&& on_message,
                                 std::invocable<> auto&& progress_hook) {
        // Termination runs as a *single* collective on the first-hop communicator. Running two sequential terminations
        // (one per hop) deadlocks: a locally-reactivated rank leaves the first-hop collective and enters the second-hop
        // one while peers are still in the first, mismatching the per-communicator allreduces. Instead we fold the
        // second hop into the first hop's counting round (additional_counts), drain the second hop every round
        // (progress + extra_round_prepare), and treat any second-hop delivery as activity that aborts the attempt.
        auto second_hop_handler = [&](Envelope<typename queue_type::message_type> auto envelope) {
            first_hop_queue_.reactivate();  // a final delivery means the system is not quiescent -> abort and retry
            on_message(std::move(envelope));
        };
        return first_hop_queue_.terminate(
            redirection_handler(on_message),
            [&] {
                second_hop_queue_.poll(second_hop_handler);
                progress_hook();
            },
            [&] { return second_hop_queue_.message_counts(); },
            [&] {
                // Drain all second-hop send buffers unconditionally. Any activity-based stop predicate
                // fails here because relay PEs are also destinations: an incoming delivery fires the
                // predicate on the very first poll, leaving the relay's forwarding backlog permanently
                // undrained — the allreduce never sees a balanced send/receive count → livelock.
                //
                // REANALYSED 2026-09-15, conclusion: leave it. should_stop exists to stop wasting
                // aggregation on an attempt that is going to be cancelled anyway, and since this
                // prepare was fused with additional_counts it only runs on attempts that reach
                // start_message_counting -- 3 per rank per iteration on rmat n18 p128, down from
                // 8,381. Expected forced flushes fall from 13,883 per rank per iteration to ~5. There
                // is essentially nothing left for a stop predicate to save, so the livelock above is
                // no longer worth trading against. Re-open only if measurements show the drain count
                // climbing back toward the terminate() call count.
                // Unlike the old single-queue design (where redirected messages re-entered the same
                // queue, amplifying work), the two-queue split means flushing second_hop_queue_ only
                // delivers messages to final destinations; there is no feedback that grows this queue.
                second_hop_queue_.flush_all_buffers_blocking(second_hop_handler, [] { return false; });
            });
    }

    /// \copydoc BufferedMessageQueue::terminate_throttled
    ///
    /// The skipped path goes through this adapter's own \ref poll, so it drives BOTH hops. Routing it to
    /// a single hop would starve the other of progress for skip_threshold calls at a stretch.
    [[nodiscard]] bool terminate_throttled(MessageHandler<typename queue_type::message_type> auto&& on_message,
                                           std::size_t skip_threshold = 1) {
        if (skip_threshold > 1 && (terminate_call_count_++ % skip_threshold) != 0) {
            poll(on_message);
            return false;
        }
        return terminate(std::forward<decltype(on_message)>(on_message));
    }

    // bool probe_for_messages(MessageHandler<typename queue_type::message_type> auto&& on_message) {
    //     first_hop_queue_.probe_for_messages(redirection_handler(std::forward<decltype(on_message)>(on_message)));
    //     return second_hop_queue_.probe_for_messages(std::forward<decltype(on_message)>(on_message));
    // }

    // bool probe_for_one_message(MessageHandler<typename queue_type::message_type> auto&& on_message,
    //                            PEID source = MPI_ANY_SOURCE,
    //                            int tag = MPI_ANY_TAG) {
    //     return first_hop_queue_.probe_for_one_message(std::forward<decltype(on_message)>(on_message), source, tag) ||
    //            second_hop_queue_.probe_for_one_message(std::forward<decltype(on_message)>(on_message), source, tag);
    // }

    void global_threshold_bytes(std::size_t new_threshold,
                                MessageHandler<typename queue_type::message_type> auto&& on_message) {
        first_hop_queue_.global_threshold_bytes(new_threshold,
                                                redirection_handler(std::forward<decltype(on_message)>(on_message)));
        second_hop_queue_.global_threshold_bytes(new_threshold, std::forward<decltype(on_message)>(on_message));
    }

    void local_threshold_bytes(std::size_t new_threshold,
                               MessageHandler<typename queue_type::message_type> auto&& on_message) {
        first_hop_queue_.local_threshold_bytes(new_threshold,
                                               redirection_handler(std::forward<decltype(on_message)>(on_message)));
        second_hop_queue_.local_threshold_bytes(new_threshold, std::forward<decltype(on_message)>(on_message));
    }

    [[nodiscard]] PEID rank() const {
        return first_hop_queue_.rank();
    }

    [[nodiscard]] PEID size() const {
        return first_hop_queue_.size();
    }

    /// if this mode is active, no incoming messages will cancel the termination process
    /// this allows using the queue as a somewhat async sparse-all-to-all
    void synchronous_mode(bool use_it = true) {
        first_hop_queue_.synchronous_mode(use_it);
        second_hop_queue_.synchronous_mode(use_it);
    }

    auto num_allocated_buffers() {
        return first_hop_queue_.num_allocated_buffers();
    }

    [[nodiscard]] std::size_t num_termination_rounds() const {
        return first_hop_queue_.num_termination_rounds();
    }

    [[nodiscard]] queue_type const& first_hop_queue() const {
        return first_hop_queue_;
    }

    [[nodiscard]] queue_type const& second_hop_queue() const {
        return second_hop_queue_;
    }

private:
    /// Distinct next-hop destinations the first-hop queue aggregates to. Every user message enters the first hop;
    /// same-group receivers are reached directly (group_size), cross-group receivers go via one proxy per other group
    /// (num_groups). Upper-bounded by their sum.
    static std::size_t first_hop_fan_out(Indirector const& indirector) {
        return static_cast<std::size_t>(indirector.group_size()) + static_cast<std::size_t>(indirector.num_groups());
    }

    /// Distinct final receivers a proxy forwards to on the second hop: the members of its own group (group_size).
    static std::size_t second_hop_fan_out(Indirector const& indirector) {
        return static_cast<std::size_t>(indirector.group_size());
    }

    /// Derive topology-aware buffering defaults for one hop from that hop's fan-out.
    /// Indirection bounds a hop's distinct destinations to `fan_out`, which is O(sqrt(p)) for a
    /// square grid — this is what keeps startup overhead (live MPI partners) tractable at scale.
    /// Delegates to apply_fan_out_defaults, which respects any values the user explicitly set.
    static Config derive_indirection_config(Config config, std::size_t fan_out) {
        return apply_fan_out_defaults(std::move(config), fan_out);
    }

    auto redirection_handler(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return [&](Envelope<typename queue_type::message_type> auto envelope) {
            KASSERT(envelope.receiver < this->size());
            bool should_redirect = indirection_.should_redirect(envelope.sender, envelope.receiver);
            if (should_redirect) {
                post_message_blocking(std::move(envelope.message), envelope.receiver, envelope.sender,
                                      envelope.receiver, envelope.tag, std::forward<decltype(on_message)>(on_message),
                                      /* direct_send = */ true);
            } else {
                KASSERT(envelope.receiver == this->rank());
                on_message(std::move(envelope));
            }
        };
    }
    Indirector indirection_;
    std::size_t terminate_call_count_ = 0;
    bool relay_drains_first_hop_ = true;
};

}  // namespace briefkasten
