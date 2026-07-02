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
        first_hop_queue_.max_num_aggregation_buffers(first_cfg.max_num_aggregation_buffers);
        first_hop_queue_.send_backlog_capacity(first_cfg.send_backlog_capacity);
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
            // While blocked waiting for a second-hop send to complete, keep draining the first-hop queue. Otherwise
            // peers blocked on the first hop never receive (and thus complete) the sends we are waiting on -> deadlock.
            return second_hop_queue_.post_message_blocking(
                std::forward<decltype(message)>(message), next_hop, envelope_sender, envelope_receiver, tag, on_message,
                [&] { first_hop_queue_.poll(redirection_handler(on_message)); });
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
                // Stop as soon as new work arrives (first-hop receive, or a second-hop delivery which
                // second_hop_handler funnels into first_hop_queue_.reactivate()): the attempt will abort anyway, so
                // don't force out small, not-yet-full second-hop buffers.
                second_hop_queue_.flush_all_buffers_blocking(second_hop_handler, [&] {
                    return first_hop_queue_.termination_state() == TerminationState::active;
                });
            });
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

    /// Derive topology-aware buffering defaults for one hop from that hop's fan-out, leaving any field the caller set
    /// explicitly (i.e. that differs from the library default) untouched.
    ///
    /// Indirection bounds a hop's distinct destinations to `fan_out`, so unlike the un-indirected case we can afford a
    /// buffer per destination. We size for double buffering: one buffer per destination filling while its predecessor
    /// drains. That second buffer only materializes if the send pipeline is deep enough to hold `fan_out` sends
    /// outstanding, which is why the backlog is set to `fan_out`:
    ///   max_num_aggregation_buffers = fan_out (filling) + fan_out (backlog) + num_request_slots (in flight).
    static Config derive_indirection_config(Config config, std::size_t fan_out) {
        Config const defaults;
        if (config.send_backlog_capacity == defaults.send_backlog_capacity) {
            config.send_backlog_capacity = fan_out;
        }
        if (config.max_num_aggregation_buffers == defaults.max_num_aggregation_buffers) {
            config.max_num_aggregation_buffers = (2 * fan_out) + config.num_request_slots;
        }
        return config;
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
};

}  // namespace briefkasten
