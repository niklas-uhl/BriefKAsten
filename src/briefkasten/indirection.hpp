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
#include <kassert/kassert.hpp>
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
class IndirectionAdapter : public BufferedQueueType {
private:
    using queue_type = BufferedQueueType;
    using MessageType = typename queue_type::message_type;

public:
    IndirectionAdapter(BufferedQueueType queue, Indirector indirector)
        : BufferedQueueType(std::move(queue)), indirection_(std::move(indirector)) {}

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
        if (!direct_send) {
            next_hop = indirection_.next_hop(envelope_sender, envelope_receiver);
        }
        return queue_type::post_message(std::forward<decltype(message)>(message), next_hop, envelope_sender,
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
        if (!direct_send) {
            next_hop = indirection_.next_hop(envelope_sender, envelope_receiver);
        }
        return queue_type::post_message_blocking(std::forward<decltype(message)>(message), next_hop, envelope_sender,
                                                 envelope_receiver, tag,
                                                 redirection_handler(std::forward<decltype(on_message)>(on_message)));
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
        return queue_type::poll(redirection_handler(std::forward<decltype(on_message)>(on_message)));
    }

    auto poll_throttled(MessageHandler<MessageType> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD) {
        return queue_type::poll_throttled(redirection_handler(std::forward<decltype(on_message)>(on_message)),
                                          poll_skip_threshold);
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
        return queue_type::terminate(redirection_handler(std::forward<decltype(on_message)>(on_message)),
                                     progress_hook);
    }

    bool probe_for_messages(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return queue_type::probe_for_messages(redirection_handler(std::forward<decltype(on_message)>(on_message)));
    }

    bool probe_for_one_message(MessageHandler<typename queue_type::message_type> auto&& on_message,
                               PEID source = MPI_ANY_SOURCE,
                               int tag = MPI_ANY_TAG) {
        return queue_type::probe_for_one_message(redirection_handler(std::forward<decltype(on_message)>(on_message)),
                                                 source, tag);
    }

    void global_threshold_bytes(std::size_t new_threshold,
                                MessageHandler<typename queue_type::message_type> auto&& on_message) {
        queue_type::global_threshold_bytes(new_threshold,
                                           redirection_handler(std::forward<decltype(on_message)>(on_message)));
    }

    void local_threshold_bytes(std::size_t new_threshold,
                               MessageHandler<typename queue_type::message_type> auto&& on_message) {
        queue_type::local_threshold_bytes(new_threshold,
                                          redirection_handler(std::forward<decltype(on_message)>(on_message)));
    }

private:
    auto redirection_handler(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return [&](Envelope<typename queue_type::message_type> auto envelope) {
            KASSERT(envelope.receiver < this->size());
            bool should_redirect = indirection_.should_redirect(envelope.sender, envelope.receiver);
            if (should_redirect) {
                post_message_blocking(std::move(envelope.message), envelope.receiver, envelope.sender,
                                      envelope.receiver, envelope.tag, std::forward<decltype(on_message)>(on_message));
            } else {
                KASSERT(envelope.receiver == this->rank());
                on_message(std::move(envelope));
            }
        };
    }
    Indirector indirection_;
};

}  // namespace briefkasten
