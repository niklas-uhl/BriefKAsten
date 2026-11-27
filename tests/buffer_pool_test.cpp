#include <gtest/gtest.h>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/communicator.hpp>

#include <cstddef>
#include <functional>
#include <vector>

#include "briefkasten/aggregators.hpp"
#include "briefkasten/grid_indirection.hpp"
#include "briefkasten/indirection.hpp"
#include "briefkasten/queue_builder.hpp"

namespace {
// 256 B buffers (64 ints) and messages of 10 ints (12 with the envelope): a destination's buffer overflows every 6th
// message, so each rank flushes hundreds of buffers per destination through the overflow path. Before the fix every
// such flush put a moved-from shell into the free list, which grew past the pool.
constexpr std::size_t THRESHOLD_BYTES = 256;
constexpr std::size_t MESSAGE_SIZE = 10;
constexpr std::size_t MESSAGES_PER_RECEIVER = 2000;

std::size_t global_sum(kamping::Communicator<> const& comm, std::size_t value) {
    return comm.allreduce_single(kamping::params::send_buf(value), kamping::params::op(std::plus<>{}));
}

/// Every rank sends MESSAGES_PER_RECEIVER messages to every rank, round robin, then terminates. Checks that every
/// message arrives and that the aggregation buffer pool stayed within its cap: the free list never holds more
/// buffers than the pool has, and the pool no more than the cap plus the proxy's overdraft.
template <typename Queue>
void run_and_check(kamping::Communicator<> const& comm, Queue& queue) {
    auto const& base = [&]() -> auto const& {
        if constexpr (requires { queue.queue(); }) {
            return queue.queue();
        } else {
            return queue;
        }
    }();
    std::size_t received = 0;
    auto on_message = [&](auto envelope) {
        received++;
        EXPECT_EQ(envelope.message.size(), MESSAGE_SIZE);
    };
    std::vector<int> const message(MESSAGE_SIZE, comm.rank_signed());
    for (std::size_t i = 0; i < MESSAGES_PER_RECEIVER; ++i) {
        for (int receiver = 0; receiver < comm.size_signed(); ++receiver) {
            queue.post_message_blocking(std::vector<int>(message), receiver, on_message);
            EXPECT_LE(base.num_free_buffers(), base.num_allocated_buffers());
        }
        queue.poll(on_message);
    }
    while (!queue.terminate(on_message)) {
    }
    // at least one overflow per 6 messages to one receiver, i.e. hundreds of flushes through the overflow path
    EXPECT_GE(base.num_overflows(), MESSAGES_PER_RECEIVER / 6);
    EXPECT_LE(base.num_free_buffers(), base.num_allocated_buffers());
    EXPECT_LE(base.num_allocated_buffers(), base.max_num_aggregation_buffers() + base.redirect_overdraft());
    // only a proxy draws from the reserve, and forwarded payload bounds it
    EXPECT_LE(base.peak_reserve_buffers(), base.redirect_pool_ceiling());
    EXPECT_EQ(global_sum(comm, received), MESSAGES_PER_RECEIVER * comm.size() * comm.size());
}

briefkasten::Config small_buffers(bool flow_control) {
    briefkasten::Config conf;
    conf.local_threshold_bytes = THRESHOLD_BYTES;
    if (!flow_control) {
        conf.num_credit_buffers = 0;
    }
    return conf;
}

auto direct_queue(kamping::Communicator<> const& comm, briefkasten::Config const& conf) {
    return briefkasten::BufferedMessageQueueBuilder<int>(comm.mpi_communicator(), conf)
        .with_merger(briefkasten::aggregation::EnvelopeSerializationMerger{})
        .with_splitter(briefkasten::aggregation::EnvelopeSerializationSplitter<int>{})
        .build();
}
}  // namespace

TEST(BufferPoolTest, direct) {
    kamping::Communicator<> comm;
    auto queue = direct_queue(comm, small_buffers(/*flow_control=*/true));
    run_and_check(comm, queue);
}

TEST(BufferPoolTest, direct_without_flow_control) {
    kamping::Communicator<> comm;
    auto queue = direct_queue(comm, small_buffers(/*flow_control=*/false));
    run_and_check(comm, queue);
}

TEST(BufferPoolTest, direct_global_flush) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.flush_strategy = briefkasten::FlushStrategy::global;
    auto queue = direct_queue(comm, conf);
    run_and_check(comm, queue);
}

TEST(BufferPoolTest, grid) {
    kamping::Communicator<> comm;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, small_buffers(/*flow_control=*/true)),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}

// One credit buffer per peer: buffers park for credit all the time, so the proxies draw from the reserve while
// every rank keeps posting its own messages to the same destinations.
TEST(BufferPoolTest, grid_small_credit_window) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = 1;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}

TEST(BufferPoolTest, grid_global_flush_small_credit_window) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = 1;
    conf.flush_strategy = briefkasten::FlushStrategy::global;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}

namespace {
// Config::separate_forwarding, with the default credit window and with one buffer per peer: everything arrives, and
// the reserve stays within the ceiling for the mode (also checked at every acquisition by a KASSERT). Own posts wait
// for the reserve only with shared buffers.
void run_grid(bool separate_forwarding, std::size_t num_credit_buffers) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = num_credit_buffers;
    conf.separate_forwarding = separate_forwarding;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
    if (separate_forwarding) {
        EXPECT_EQ(queue.queue().num_reserve_waits(), 0);
    }
}
}  // namespace

TEST(BufferPoolTest, grid_shared_buffers) {
    run_grid(false, briefkasten::DEFAULT_NUM_CREDIT_BUFFERS);
}

TEST(BufferPoolTest, grid_shared_buffers_small_credit_window) {
    run_grid(false, 1);
}
