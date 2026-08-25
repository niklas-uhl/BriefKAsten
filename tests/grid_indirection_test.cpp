#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "briefkasten/grid_indirection.hpp"

// This test does not run under MPI: GridIndirectionScheme::for_testing() builds a scheme from an explicit
// (rank, comm_size) pair, bypassing MPI_Comm_rank/MPI_Comm_size, so we can sweep comm_size values (in particular
// non-square ones, which are the overwhelming majority of real MPI job sizes) that no realistic MPI test harness
// would spin up ranks for.

namespace {

/// For every rank in a communicator of size `comm_size`, and every other rank as a destination, `next_hop` must
/// return a rank strictly within [0, comm_size) -- it is used directly as an MPI_Isend destination. Before the fix in
/// get_proxy(), a fallback proxy candidate was only bounds-checked via KASSERT (compiled out under NDEBUG, the
/// normal release-build mode), so a bad candidate could silently escape as an out-of-range destination.
void expect_next_hop_always_in_range(int comm_size) {
    for (int from = 0; from < comm_size; ++from) {
        auto scheme = briefkasten::GridIndirectionScheme::for_testing(from, comm_size);
        for (int to = 0; to < comm_size; ++to) {
            if (!scheme.should_redirect(from, to)) {
                continue;
            }
            auto hop = scheme.next_hop(from, to);
            ASSERT_GE(hop, 0) << "comm_size=" << comm_size << " from=" << from << " to=" << to;
            ASSERT_LT(hop, comm_size) << "comm_size=" << comm_size << " from=" << from << " to=" << to;
        }
    }
}

}  // namespace

// NOLINTBEGIN(*-magic-numbers)

// Regression test for the specific class of comm sizes discussed in the bugfix: non-square sizes close to a grid
// boundary (grid_size_ = round(sqrt(comm_size))), including sizes cited while investigating the original report
// (96, 768, 6144) and their immediate neighbors, where the grid's last row is incomplete.
TEST(GridIndirection, next_hop_in_range_for_reported_sizes) {
    for (int comm_size : {95, 96, 97, 767, 768, 769, 6143, 6144, 6145}) {
        expect_next_hop_always_in_range(comm_size);
    }
}

// Broader sweep across small-to-medium comm sizes, covering perfect squares, their neighbors, and everything in
// between, so we exhaustively cover every grid shape (including maximally "incomplete last row" cases) that can
// occur for grid_size_ = round(sqrt(comm_size)).
TEST(GridIndirection, next_hop_in_range_exhaustive_sweep) {
    for (int comm_size = 1; comm_size <= 500; ++comm_size) {
        expect_next_hop_always_in_range(comm_size);
    }
}

// NOLINTEND(*-magic-numbers)
