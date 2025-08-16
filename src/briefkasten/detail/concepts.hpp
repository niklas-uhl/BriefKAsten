// Copyright (c) 2021-2025 Tim Niklas Uhl
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights too
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

#include <concepts>  // IWYU pragma: keep
#include <kamping/mpi_datatype.hpp>
#include <ranges>
#include <vector>
#include "./definitions.hpp"

namespace briefkasten {

template <typename T>
concept MPIType = kamping::has_static_type_v<T>;

/// @brief a buffer directly sendable by \c std::data() and \c std::size() to an MPI call.
template <typename Container, typename ValueType = void>
concept MPIBuffer = std::ranges::sized_range<Container> && std::ranges::contiguous_range<Container> &&
                    MPIType<std::ranges::range_value_t<Container>> &&
                    (std::same_as<void, ValueType> || std::same_as<ValueType, std::ranges::range_value_t<Container>>);

/// @brief messages which need not necessarily satisfy \ref MPIType.
template <typename Range, typename MessageType = void>
concept MessageRange =
    std::ranges::input_range<Range> && std::ranges::sized_range<Range> &&
    (std::same_as<void, MessageType> || std::same_as<MessageType, std::ranges::range_value_t<Range>>);

template <typename Range, typename MessageType = void>
concept InputMessageRange = !std::is_lvalue_reference_v<Range> && MessageRange<Range, MessageType>;

static_assert(InputMessageRange<std::vector<int>, int>);
static_assert(InputMessageRange<std::ranges::single_view<int>, int>);
static_assert(InputMessageRange<std::ranges::empty_view<int>, int>);

enum class EnvelopeType { tag, full };

template <typename E, typename MessageType = void>
concept Envelope = requires(E envelope) {
    { envelope.tag } -> std::same_as<int&>;
    { envelope.message } -> MessageRange<MessageType>;
    typename E::message_range_type;
    typename E::message_value_type;
    requires std::same_as<decltype(E::type), const EnvelopeType>;
} && MessageRange<typename E::message_range_type, MessageType>;

template <MessageRange MessageRangeType>
struct MessageEnvelope {
    explicit MessageEnvelope(MessageRangeType message, PEID sender, PEID receiver, int tag)
        : message(std::move(message)), sender(sender), receiver(receiver), tag(tag) {}
    MessageEnvelope() = default;
    // MessageEnvelope(MessageEnvelope const&) = delete;
    // MessageEnvelope(MessageEnvelope&&) = default;
    // MessageEnvelope& operator=(MessageEnvelope const&) = delete;
    // MessageEnvelope& operator=(MessageEnvelope&&) = default;
    using message_range_type = MessageRangeType;
    using message_value_type = std::ranges::range_value_t<message_range_type>;
    static constexpr EnvelopeType type = EnvelopeType::full;
    MessageRangeType message;
    PEID sender;
    PEID receiver;
    int tag;
};

static_assert(Envelope<MessageEnvelope<std::vector<int>>, int>);
static_assert(Envelope<MessageEnvelope<std::vector<int>>>);

template <typename MessageFunc,
          typename MessageDataType,
          typename MessageContainerType = std::ranges::empty_view<MessageDataType>>
concept MessageHandler = MessageRange<MessageContainerType, MessageDataType> &&
                         requires(MessageFunc on_message, MessageEnvelope<MessageContainerType> envelope) {
                             { on_message(std::move(envelope)) };
                         };

template <typename Func, typename MessageContainerType>
concept SendFinishedCallback =
    std::invocable<Func, std::size_t> || std::invocable<Func, std::size_t, MessageContainerType>;

template <typename Fn, typename BufferMapType>
concept OverflowHandler = requires(Fn handle_overflow, typename BufferMapType::iterator it) { handle_overflow(it); };

template <typename Fn, typename BufferType>
concept BufferProvider = requires(Fn get_new_buffer) {
    { get_new_buffer() } -> std::same_as<BufferType>;
};

namespace aggregation {
template <typename MergerType, typename MessageType, typename BufferContainer>
concept Merger = requires(MergerType merge,
                          BufferContainer& buffer,
                          PEID buffer_destination,
                          PEID my_rank,
                          MessageEnvelope<std::ranges::empty_view<MessageType>> envelope) {
    merge(buffer, buffer_destination, my_rank, std::move(envelope));
};

template <typename MergerType, typename MessageType, typename BufferContainer>
concept EstimatingMerger = Merger<MergerType, MessageType, BufferContainer> &&
                           requires(MergerType merge,
                                    BufferContainer const& buffer,
                                    PEID buffer_destination,
                                    PEID my_rank,
                                    MessageEnvelope<std::ranges::empty_view<MessageType>> const& envelope) {
                               {
                                   merge.estimate_new_buffer_size(buffer, buffer_destination, my_rank, envelope)
                               } -> std::same_as<size_t>;
                           };

template <typename Range, typename T>
concept SplitRange = std::ranges::forward_range<Range> && Envelope<std::ranges::range_value_t<Range>, T>;

template <typename SplitterType, typename MessageType, typename BufferContainer>
concept Splitter = requires(SplitterType split, PEID buffer_origin, PEID my_rank, BufferContainer const& buffer) {
    { split(buffer, buffer_origin, my_rank) } -> SplitRange<MessageType>;
};

template <typename BufferCleanerType, typename BufferContainer>
concept BufferCleaner = requires(BufferCleanerType pre_send_cleanup, BufferContainer& buffer, PEID receiver) {
    pre_send_cleanup(buffer, receiver);
};
}  // namespace aggregation
}  // namespace briefkasten
