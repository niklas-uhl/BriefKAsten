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
          second_hop_queue_(second_hop_queue_comm_.mpi_communicator(), first_hop_queue_.config()),
          indirection_(std::move(indirector)) {}

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

private:
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
