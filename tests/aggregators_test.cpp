#include <gtest/gtest.h>

#include <cstdint>
#include <ranges>
#include <span>
#include <vector>

#include "briefkasten/aggregators.hpp"

namespace {
using briefkasten::MessageEnvelope;
using briefkasten::PEID;
using briefkasten::aggregation::EnvelopeSerializationMerger;
using briefkasten::aggregation::EnvelopeSerializationSplitter;

template <typename Message, typename Buffer>
void merge(std::vector<Buffer>& buffer, std::vector<Message> const& message, PEID receiver) {
    EnvelopeSerializationMerger merger;
    std::size_t const expected = merger.estimate_new_buffer_size(
        buffer, receiver, 0, MessageEnvelope{std::span<Message const>(message), 0, receiver, 0});
    merger(buffer, receiver, 0, MessageEnvelope{std::span<Message const>(message), 0, receiver, 0});
    EXPECT_EQ(buffer.size(), expected);
}
}  // namespace

/// Same message and buffer type: each message comes out as a contiguous view into the buffer.
TEST(EnvelopeSerializationTest, round_trip_without_conversion_is_contiguous) {
    std::vector<std::vector<std::uint64_t>> const messages = {{1, 2, 3}, {}, {42}, {7, 8, 9, 10, 11}};
    std::vector<std::uint64_t> buffer;
    for (std::size_t i = 0; i < messages.size(); ++i) {
        merge(buffer, messages[i], static_cast<PEID>(i));
    }
    EXPECT_EQ(buffer.size(), 9U + 2 * messages.size());

    auto split = EnvelopeSerializationSplitter<std::uint64_t>{}(buffer, 0, 0);
    std::size_t i = 0;
    for (auto envelope : split) {
        static_assert(std::ranges::contiguous_range<decltype(envelope.message)>);
        ASSERT_LT(i, messages.size());
        EXPECT_EQ(envelope.receiver, static_cast<PEID>(i));
        EXPECT_TRUE(std::ranges::equal(envelope.message, messages[i]));
        if (!envelope.message.empty()) {
            // a view into the buffer, not a copy
            EXPECT_GE(envelope.message.data(), buffer.data());
            EXPECT_LE(envelope.message.data() + envelope.message.size(), buffer.data() + buffer.size());
        }
        ++i;
    }
    EXPECT_EQ(i, messages.size());
}

/// A wider buffer than the message type: values are converted on the way in and out.
TEST(EnvelopeSerializationTest, round_trip_with_conversion) {
    std::vector<std::vector<int>> const messages = {{-1, 2, -3}, {}, {1 << 30}};
    std::vector<std::int64_t> buffer;
    for (std::size_t i = 0; i < messages.size(); ++i) {
        merge(buffer, messages[i], static_cast<PEID>(i + 3));
    }
    std::size_t i = 0;
    for (auto envelope : EnvelopeSerializationSplitter<int>{}(buffer, 0, 0)) {
        ASSERT_LT(i, messages.size());
        EXPECT_EQ(envelope.receiver, static_cast<PEID>(i + 3));
        std::vector<int> const message(envelope.message.begin(), envelope.message.end());
        EXPECT_EQ(message, messages[i]);
        ++i;
    }
    EXPECT_EQ(i, messages.size());
}
