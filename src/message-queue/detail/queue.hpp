// Copyright (c) 2021-2025 Tim Niklas Uhl
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
#include <concepts>
#include <cstddef>
#include <kamping/environment.hpp>
#include <kamping/mpi_datatype.hpp>
#include <kassert/kassert.hpp>
#include <optional>
#include <utility>
#include <vector>

#include "./concepts.hpp"
#include "./receiver.hpp"
#include "./sender.hpp"
#include "./termination_counter.hpp"

namespace message_queue {

static constexpr std::size_t DEFAULT_POLL_SKIP_THRESHOLD = 100;

enum class TerminationState : std::uint8_t { active, trying_termination, terminated };

template <MPIType T,
          MPIBuffer<T> MessageContainer = std::vector<T>,
          MPIBuffer<T> ReceiveBufferContainer = std::vector<T>>
class MessageQueue {
public:
    MessageQueue(MPI_Comm comm,
                 size_t num_request_slots,
                 size_t reserved_receive_buffer_size,
                 size_t out_buffer_capacity = 0)

        : comm_(comm),
          termination_(comm),
          sender_(comm, num_request_slots, out_buffer_capacity),
          receiver_(comm, SMALL_MESSAGE_TAG, termination_, num_request_slots, reserved_receive_buffer_size),
          large_message_receiver_(comm, LARGE_MESSAGE_TAG, termination_),
          reserved_receive_buffer_size_(reserved_receive_buffer_size) {
        MPI_Comm_rank(comm_, &rank_);
        MPI_Comm_size(comm_, &size_);

        // ensure that we build the datatype as early as possible
        // cast to void to silence nodiscard warning
        static_cast<void>(kamping::mpi_datatype<T>());
    }

    MessageQueue(MessageQueue const&) = delete;

    MessageQueue(MessageQueue&& other) noexcept
        : comm_(other.comm_),
          SMALL_MESSAGE_TAG(other.SMALL_MESSAGE_TAG),
          LARGE_MESSAGE_TAG(other.LARGE_MESSAGE_TAG),
          termination_(std::move(other.termination_)),
          sender_(std::move(other.sender_)),
          receiver_(std::move(other.receiver_)),
          large_message_receiver_(other.large_message_receiver_),
          reserved_receive_buffer_size_(other.reserved_receive_buffer_size_),
          rank_(other.rank_),
          size_(other.size_),
          allow_large_messages_(other.allow_large_messages_),
          termination_state_(other.termination_state_),
          synchronous_mode_(other.synchronous_mode_) {
        receiver_.rebind_termination_counter(termination_);
        large_message_receiver_.rebind_termination_counter(termination_);
    }

    MessageQueue& operator=(MessageQueue const& other) = delete;
  
    MessageQueue& operator=(MessageQueue&& other) noexcept {
        comm_ = other.comm_;
        SMALL_MESSAGE_TAG = other.SMALL_MESSAGE_TAG;
        LARGE_MESSAGE_TAG = other.LARGE_MESSAGE_TAG;
        termination_ = std::move(termination_);
        sender_ = std::move(other.sender_);
        receiver_ = std::move(other.receiver_);
	large_message_receiver_ = std::move(large_message_receiver_);
        reserved_receive_buffer_size_ = other.reserved_receive_buffer_size_;
        rank_ = other.rank_;
        size_ = other.size_;
        allow_large_messages_ = other.allow_large_messages_;
        termination_state_ = other.termination_state_;
        synchronous_mode_ = other.synchronous_mode_;
	receiver_.rebind_termination_counter(termination_);
	large_message_receiver_.rebind_termination_counter(termination_);
    }

    /// Post a message to the message queue
    /// Posting a message may fail if the message box is full and no send slots are available.
    /// @return an optional containing the request id if the message was successfully posted, otherwise nullopt
    auto post_message(MessageContainer&& message, PEID receiver) -> std::optional<std::size_t> {
        int tag = SMALL_MESSAGE_TAG;
        if (message.size() > reserved_receive_buffer_size_) {
            if (!allow_large_messages_) {
                throw std::runtime_error{"Large messages not allowed, enable them using allow_large_messages"};
            }
            tag = LARGE_MESSAGE_TAG;
        }
        std::optional<std::size_t> receipt = sender_.enqueue_for_sending(std::move(message), receiver, tag);
        if (receipt.has_value()) {
            termination_.track_send();
        }
        return receipt;
    }

    /// For single element messages
    auto post_message(T message, PEID receiver) -> std::optional<std::size_t> {
        MessageContainer message_vector{std::move(message)};
        return post_message(std::move(message_vector), receiver);
    }

    auto poll(MessageHandler<T, MessageContainer> auto&& on_message) -> std::optional<std::pair<bool, bool>> {
        return poll(on_message, [](std::size_t) {});
    }

    auto poll(MessageHandler<T, MessageContainer> auto&& on_message,
              SendFinishedCallback<MessageContainer> auto&& on_finished_sending)
        -> std::optional<std::pair<bool, bool>> {
        bool received_large_message = false;
        if (allow_large_messages_) {
            received_large_message =
                large_message_receiver_.probe_for_one_message(std::forward<decltype(on_message)>(on_message));
        }
        bool received_something =
            receiver_.probe_for_messages(std::forward<decltype(on_message)>(on_message)) || received_large_message;
        if (received_something) {
            reactivate();
        }
        bool send_finished_something =
            sender_.progress_sending(std::forward<decltype(on_finished_sending)>(on_finished_sending));
        if (send_finished_something || received_something) {
            return std::pair{send_finished_something, received_something};
        }
        return std::nullopt;
    }

    auto poll_throttled(MessageHandler<T, MessageContainer> auto&& on_message,
                        SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD)
        -> std::optional<std::pair<bool, bool>> {
        static std::size_t poll_count = 0;
        if (poll_count % poll_skip_threshold == 0) {
            poll_count++;
            return poll(std::forward<decltype(on_message)>(on_message),
                        std::forward<decltype(on_finished_sending)>(on_finished_sending));
        }
        return std::nullopt;
    }

    auto poll_throttled(MessageHandler<T, MessageContainer> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD) {
        return poll_throttled(std::forward<decltype(on_message)>(on_message), [](std::size_t) {}, poll_skip_threshold);
    }

    void reactivate() {
        if (synchronous_mode_) {
            return;
        }
        termination_state_ = TerminationState::active;
    }

    [[nodiscard]] bool terminate(MessageHandler<T, MessageContainer> auto&& on_message,
                                 SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
                                 std::invocable<> auto&& before_next_message_counting_round_hook,
                                 std::invocable<> auto&& progress_hook) {
        termination_state_ = TerminationState::trying_termination;
        while (true) {
            before_next_message_counting_round_hook();
            if (termination_state_ == TerminationState::active) {
                return false;
            }
            poll_until_message_box_empty(std::forward<decltype(on_message)>(on_message),
                                         std::forward<decltype(on_finished_sending)>(on_finished_sending),
                                         [&] { return termination_state_ == TerminationState::active; });
            if (termination_state_ == TerminationState::active) {
                return false;
            }
            termination_.start_message_counting();
            // poll at least once, so we don't miss any messages
            // if the the message box is empty upon calling this function
            // we never get to poll if message counting finishes instantly
            do {
                poll(std::forward<decltype(on_message)>(on_message),
                     std::forward<decltype(on_finished_sending)>(on_finished_sending));
                progress_hook();
                if (termination_state_ == TerminationState::active) {
                    return false;
                }
            } while (!termination_.message_counting_finished());
            if (termination_.terminated()) {
                termination_state_ = TerminationState::terminated;
                return true;
            }
        }
    }

    [[nodiscard]] bool terminate(MessageHandler<T, MessageContainer> auto&& on_message,
                                 std::invocable<> auto&& before_next_message_counting_round_hook) {
        return terminate(
            std::forward<decltype(on_message)>(on_message), [](std::size_t) {},
            std::forward<decltype(before_next_message_counting_round_hook)>(before_next_message_counting_round_hook),
            []() {});
    }

    bool terminate(MessageHandler<T, MessageContainer> auto&& on_message) {
        return terminate(on_message, [] {});
    }

    /// if this mode is active, no incoming messages will cancel the termination process
    /// this allows using the queue as a somewhat async sparse-all-to-all
    void synchronous_mode(bool use_it = true) {
        synchronous_mode_ = use_it;
    }

    [[nodiscard]] size_t reserved_receive_buffer_size() const {
        return reserved_receive_buffer_size_;
    }

    void resize_receive_buffers(std::size_t new_size, MessageHandler<T, MessageContainer> auto&& on_message) {
        receiver_.resize_buffers(new_size, std::forward<decltype(on_message)>(on_message));
        reserved_receive_buffer_size_ = new_size;
    }

    void allow_large_messages(bool allow = true) {
        allow_large_messages_ = allow;
    }

    [[nodiscard]] PEID rank() const {
        return rank_;
    }

    [[nodiscard]] PEID size() const {
        return size_;
    }

    [[nodiscard]] MPI_Comm communicator() const {
        return comm_;
    }

    [[nodiscard]] bool has_send_capacity() const {
        return sender_.has_capacity();
    }

    [[nodiscard]] TerminationState termination_state() const {
        return termination_state_;
    }

private:
    void poll_until_message_box_empty(
        MessageHandler<T, MessageContainer> auto&& on_message,
        SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
        std::predicate<> auto&& should_stop_polling = [] { return false; }) {
        while (sender_.pending_messages() > 0) {
            poll(std::forward<decltype(on_message)>(on_message),
                 std::forward<decltype(on_finished_sending)>(on_finished_sending));
            if (should_stop_polling()) {
                return;
            }
        }
    }

    MPI_Comm comm_;
    int SMALL_MESSAGE_TAG = kamping::Environment<>::tag_upper_bound() - 1;
    int LARGE_MESSAGE_TAG = kamping::Environment<>::tag_upper_bound() - 2;
    internal::TerminationCounter termination_;
    Sender<MessageContainer> sender_;
    PersistentReceiver<ReceiveBufferContainer> receiver_;
    AllocatingProbeReceiver<ReceiveBufferContainer> large_message_receiver_;
    size_t reserved_receive_buffer_size_;
    PEID rank_ = 0;
    PEID size_ = 0;
    bool allow_large_messages_ = false;
    TerminationState termination_state_ = TerminationState::active;
    bool synchronous_mode_ = false;
};

}  // namespace message_queue
