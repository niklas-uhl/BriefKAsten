#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/communicator.hpp>

#include <algorithm>
#include <random>
#include <vector>

#include "briefkasten/aggregators.hpp"
#include "briefkasten/grid_indirection.hpp"
#include "briefkasten/indirection.hpp"
#include "briefkasten/queue_builder.hpp"

namespace {
constexpr std::size_t NUM_LOCAL_ELEMENTS = 200'000;

/// An all-to-all over a redirecting grid, driven to completion and checked for exact delivery.
///
/// Exact delivery is the point. Redirected payload that has been received but not yet forwarded is counted as
/// received and not as sent, so a termination protocol that looks only at the buffer balance can fire while
/// data is still sitting in a proxy's buffer -- silent message loss, invisible at verify-level 0, and the
/// same class of bug as the grid_alltoallv data loss. A global count that comes up short is what catches it.
struct Outcome {
    std::size_t received = 0;
    std::size_t parked_for_credit = 0;
    std::size_t redirect_buffer_stalls = 0;
    std::size_t overflow_capacity_waits = 0;
    std::size_t drain_capacity_waits = 0;
    std::size_t grants_sent = 0;
    std::size_t pending_at_end = 0;
    bool flow_control = false;
};

Outcome run_alltoall(kamping::Communicator<> const& comm, briefkasten::Config conf,
                     bool poll_while_posting = true) {
    std::vector<int> data(NUM_LOCAL_ELEMENTS);
    std::default_random_engine generator{static_cast<std::default_random_engine::result_type>(comm.rank())};
    std::uniform_int_distribution<int> distribution(0, comm.size_signed() - 1);
    std::ranges::generate(data, [&]() { return distribution(generator); });

    briefkasten::IndirectionAdapter queue{
        briefkasten::BufferedMessageQueueBuilder<int>(comm.mpi_communicator(), conf)
            .with_merger(briefkasten::aggregation::EnvelopeSerializationMerger{})
            .with_splitter(briefkasten::aggregation::EnvelopeSerializationSplitter<int>{})
            .build(),
        briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};

    Outcome outcome;
    auto on_message = [&](auto envelope) {
        for (auto value : envelope.message) {
            EXPECT_EQ(value, comm.rank_signed());
            outcome.received++;
        }
    };
    for (auto& element : data) {
        queue.post_message_blocking(element, element, on_message);
        if (poll_while_posting) {
            queue.poll_throttled(on_message);
        }
    }
    while (!queue.terminate(on_message)) {
    }

    auto const& q = queue.queue();
    outcome.parked_for_credit = q.num_parked_for_credit();
    outcome.redirect_buffer_stalls = q.num_redirect_buffer_stalls();
    outcome.overflow_capacity_waits = q.num_overflow_capacity_waits();
    outcome.drain_capacity_waits = q.num_drain_capacity_waits();
    outcome.grants_sent = q.num_grants_sent();
    outcome.pending_at_end = q.pending_elements();
    outcome.flow_control = q.flow_control_enabled();
    return outcome;
}

std::size_t global_sum(kamping::Communicator<> const& comm, std::size_t value) {
    return comm.allreduce_single(kamping::params::send_buf(value), kamping::params::op(std::plus<>{}));
}
}  // namespace

/// Flow control is on by default under indirection, and the proxy must never block.
TEST(FlowControlTest, indirect_alltoall_delivers_everything) {
    kamping::Communicator<> comm;
    auto outcome = run_alltoall(comm, briefkasten::Config{});

    EXPECT_TRUE(outcome.flow_control);
    EXPECT_EQ(global_sum(comm, outcome.received), NUM_LOCAL_ELEMENTS * comm.size());
    EXPECT_EQ(outcome.pending_at_end, 0U);
    // The whole point: with credits, nothing ever waits for send capacity. Both counters sit on blocking
    // loops, and the overflow one is on the redirection handler's path -- a proxy spinning there is a receive
    // slot left disarmed, which is the defect this exists to remove. They must be exactly zero, not small.
    EXPECT_EQ(outcome.overflow_capacity_waits, 0U);
    EXPECT_EQ(outcome.drain_capacity_waits, 0U);
    EXPECT_EQ(outcome.redirect_buffer_stalls, 0U);
}

/// The A/B control: an explicit budget of 0 turns credits off and restores the blocking behaviour. It must
/// still deliver everything, or the comparison the measurements rest on is not like-for-like.
TEST(FlowControlTest, indirect_alltoall_without_flow_control) {
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    conf.num_credit_buffers = 0;
    auto outcome = run_alltoall(comm, conf);

    EXPECT_FALSE(outcome.flow_control);
    EXPECT_EQ(global_sum(comm, outcome.received), NUM_LOCAL_ELEMENTS * comm.size());
    EXPECT_EQ(outcome.pending_at_end, 0U);
}

/// A window small enough that buffers are parked and have to wait for grants. A parked buffer is the state in
/// which termination would most likely fire early and lose data.
TEST(FlowControlTest, parking_under_a_tiny_window_still_delivers_everything) {
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    conf.local_threshold_bytes = 1024;  // 256 elements per buffer
    conf.num_credit_buffers = 2;        // two buffers of credit per peer: as tight as the design allows
    // Deliberately no polling while posting. A sender that services its own inbox between posts keeps its
    // peers' windows fed and never starves -- which is what happened at 2 ranks, where the grid degenerates
    // to a single column and nothing is redirected. Withholding the poll is what forces buffers to be parked,
    // and it also exercises the intended backpressure: the sender ends up blocking on the buffer pool,
    // which is the one place it is still allowed to block.
    auto outcome = run_alltoall(comm, conf, /*poll_while_posting=*/false);

    EXPECT_TRUE(outcome.flow_control);
    EXPECT_EQ(global_sum(comm, outcome.received), NUM_LOCAL_ELEMENTS * comm.size());
    EXPECT_EQ(outcome.pending_at_end, 0U);
    EXPECT_EQ(outcome.redirect_buffer_stalls, 0U);
    EXPECT_EQ(outcome.overflow_capacity_waits, 0U);
    // Grants are volume-driven, not timing-driven -- one falls due every time half a window is consumed --
    // so any run with a peer and this much traffic must have sent some. This is what proves the protocol
    // actually ran rather than sitting inert behind an over-generous window.
    if (comm.size() > 1) {
        EXPECT_GT(global_sum(comm, outcome.grants_sent), 0U);
    }
    // Parking, unlike granting, needs the sender to actually outrun its credit, and that cannot be forced
    // at 2 ranks: round(sqrt(2)) is 1, so the grid is a single column, nothing is redirected, and the
    // sender's own overflow poll refreshes its credit before it can run out. From 3 ranks up there is a
    // redirect hop and the round trip is long enough that buffers really are parked.
    if (comm.size() >= 3) {
        EXPECT_GT(global_sum(comm, outcome.parked_for_credit), 0U);
    }
}
