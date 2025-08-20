#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <print>
#include <ranges>

#include "briefkasten/detail/view_adaptors.hpp"

// NOLINTBEGIN(*-magic-numbers)
TEST(ViewAdaptors, chunk_by_embedded_size_view) {
    std::vector<int> buf = {3, 1, 1, 1, 2, 42, 42, 5, 8, 8, 8, 8, 8};
    auto chunks =
        buf | briefkasten::chunk_by_embedded_size(0) |
        std::views::transform([](auto chunk) { return chunk | std::views::drop(1) | std::ranges::to<std::vector>(); }) |
        std::ranges::to<std::vector>();

    using namespace ::testing;
    ASSERT_THAT(chunks, SizeIs(3));
    EXPECT_THAT(chunks[0], SizeIs(3));
    EXPECT_THAT(chunks[0], Each(Eq(1)));
    EXPECT_THAT(chunks[1], SizeIs(2));
    EXPECT_THAT(chunks[1], Each(Eq(42)));
    EXPECT_THAT(chunks[2], SizeIs(5));
    EXPECT_THAT(chunks[2], Each(Eq(8)));
}
// NOLINTEND(*-magic-numbers)
