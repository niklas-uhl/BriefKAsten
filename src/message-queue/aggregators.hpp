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

#include <iterator>
#include <ranges>
#include <span>
#include "./detail/concepts.hpp"
#include "./detail/view_adaptors.hpp"

namespace message_queue::aggregation {

struct AppendMerger {
    template <MPIBuffer BufferContainer>
    void operator()(BufferContainer& buffer,
                    PEID /* buffer_destination */,
                    PEID /* my_rank */,
                    Envelope<std::ranges::range_value_t<BufferContainer>> auto envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
    }

    template <typename MessageContainer, typename BufferContainer>
    [[nodiscard]] size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                                  PEID /* buffer_destination */,
                                                  PEID /* my_rank */,
                                                  MessageEnvelope<MessageContainer> const& envelope) const {
        return buffer.size() + envelope.message.size();
    };
};
static_assert(Merger<AppendMerger, int, std::vector<int>>);
static_assert(EstimatingMerger<AppendMerger, int, std::vector<int>>);

struct NoSplitter {
    auto operator()(MPIBuffer auto const& buffer, PEID buffer_origin, PEID my_rank) const {
        MessageEnvelope envelope{std::ranges::ref_view{buffer}, buffer_origin, my_rank, 0};
        return std::ranges::single_view{std::move(envelope)};
    }
};
static_assert(Splitter<NoSplitter, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelMerger {
    SentinelMerger(BufferType sentinel) : sentinel_(sentinel) {}

    void operator()(MPIBuffer<BufferType> auto& buffer,
                    PEID /* buffer_destination */,
                    PEID /* my_rank */,
                    Envelope<BufferType> auto envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
        buffer.push_back(sentinel_);
    }

    [[nodiscard]] size_t estimate_new_buffer_size(MPIBuffer<BufferType> auto const& buffer,
                                                  PEID /* buffer_destination */,
                                                  PEID /* my_rank */,
                                                  Envelope<BufferType> auto const& envelope) const {
        return buffer.size() + envelope.message.size() + 1;
    };
    BufferType sentinel_;
};
static_assert(Merger<SentinelMerger<int>, int, std::vector<int>>);
static_assert(EstimatingMerger<SentinelMerger<int>, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelSplitter {
    SentinelSplitter(BufferType sentinel) : sentinel_(sentinel) {}

    // NOLINTNEXTLINE(*-easily-swappable-parameters)
    auto operator()(MPIBuffer<BufferType> auto const& buffer, PEID buffer_origin, PEID my_rank) const {
        return std::views::split(buffer, sentinel_) |
               std::views::transform([&, buffer_origin = buffer_origin, my_rank = my_rank](auto range) {
#ifdef MESSAGE_QUEUE_SPLIT_VIEW_IS_LAZY
                   auto size = std::ranges::distance(range);
                   auto sized_range = std::span(range.begin().base(), size);
#else
                   auto sized_range = std::move(range);
#endif
                   static_assert(std::ranges::borrowed_range<decltype(sized_range)>);
                   return MessageEnvelope{std::move(sized_range), buffer_origin, my_rank, 0};
               });
    }
    BufferType sentinel_;
};

static_assert(Splitter<SentinelSplitter<int>, int, std::vector<int>>);

template <typename EnvType, typename BufferContainer>
concept SerializableEnvelope =
    MPIBuffer<BufferContainer> && Envelope<EnvType> &&
    std::is_convertible_v<typename EnvType::message_value_type, std::ranges::range_value_t<BufferContainer>> &&
    std::is_convertible_v<PEID, std::ranges::range_value_t<BufferContainer>> &&
    std::is_convertible_v<std::size_t, std::ranges::range_value_t<BufferContainer>>;

struct EnvelopeSerializationMerger {
    template <MPIBuffer BufferContainer,
              SerializableEnvelope<BufferContainer> EnvType>  // requires that both the message value type and
    void operator()(BufferContainer& buffer, PEID /* buffer_destination */, PEID /* my_rank */, EnvType envelope) {
        using env_type = EnvType::message_value_type;
        using buffer_type = std::ranges::range_value_t<BufferContainer>;
        std::vector<buffer_type> combined(envelope.message.size() + 2);  // extra space for receiver and size
        combined[0] = static_cast<buffer_type>(envelope.receiver);
        combined[1] = static_cast<buffer_type>(envelope.message.size());
        // Copy the message into the combined vector, starting from index 2
        // and transform the message values to the buffer type.
        auto converted_message = envelope.message | std::views::transform([](env_type const& value) -> buffer_type {
                                     return static_cast<buffer_type>(value);
                                 });
        std::ranges::copy(converted_message, combined.begin() + 2);
        buffer.insert(buffer.end(), combined.begin(), combined.end());
    }
    template <MPIBuffer BufferContainer, SerializableEnvelope<BufferContainer> EnvType>
    size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                    PEID /* buffer_destination */,
                                    PEID /* my_rank */,
                                    EnvType const& envelope) const {
        return buffer.size() + envelope.message.size() + 2;  // 2 for receiver and size
    }
};

template <typename MessageType>
struct EnvelopeSerializationSplitter {
    template <MPIBuffer BufferContainer>
    auto operator()(BufferContainer const& buffer, PEID /* buffer_origin */, PEID /* my_rank */) const {
        return buffer | chunk_by_embedded_size(1) | std::views::transform([](auto chunk) {
                   PEID receiver = static_cast<PEID>(chunk[0]);
                   auto message = chunk | std::views::drop(2) |
                                  std::views::transform([](auto& elem) { return static_cast<MessageType>(elem); });
                   return MessageEnvelope{std::move(message), 0, receiver, 0};
               });
    }
};

struct NoOpCleaner {
    template <typename BufferContainer>
    void operator()(BufferContainer& /* buffer */, PEID /* buffer_destination */) const {}
};
static_assert(BufferCleaner<NoOpCleaner, std::vector<int>>);

}  // namespace message_queue::aggregation
