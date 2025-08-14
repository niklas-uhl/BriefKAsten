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
#include <span>
#include "./detail/concepts.hpp"

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
    size_t estimate_new_buffer_size(BufferContainer const& buffer,
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
                    PEID buffer_destination,
                    PEID my_rank,
                    Envelope<BufferType> auto envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
        buffer.push_back(sentinel_);
    }
    size_t estimate_new_buffer_size(MPIBuffer<BufferType> auto const& buffer,
                                    PEID buffer_destination,
                                    PEID my_rank,
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

    auto operator()(MPIBuffer<BufferType> auto const& buffer, PEID buffer_origin, PEID my_rank) const {
        return std::views::split(buffer, sentinel_) |
               std::views::transform([&, buffer_origin = buffer_origin, my_rank = my_rank](auto&& range) {
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

struct NoOpCleaner {
    template <typename BufferContainer>
    void operator()(BufferContainer& /* buffer */, PEID) const {}
};
static_assert(BufferCleaner<NoOpCleaner, std::vector<int>>);

}  // namespace message_queue::aggregation
