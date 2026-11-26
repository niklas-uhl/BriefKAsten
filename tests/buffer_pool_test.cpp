#include <gtest/gtest.h>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/collectives/barrier.hpp>
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
    // the tests share the communicator and the tags: let every rank destroy the previous test's queue first
    comm.barrier();
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

// A credit window of two buffers per peer: buffers park for credit all the time, so the proxies draw from the
// reserve while every rank keeps posting its own messages to the same destinations. (One buffer hangs on this base,
// 5664f7f, with or without the pool fix: the credit stall fixed later by #24.)
TEST(BufferPoolTest, grid_small_credit_window) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = 2;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}

TEST(BufferPoolTest, grid_global_flush_small_credit_window) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = 2;
    conf.flush_strategy = briefkasten::FlushStrategy::global;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}

// The experiment switch Config::bounded_pool = false restores the pool handling from before #29: everything is
// still delivered, and the free list outgrows the pool again (the leak this suite guards against otherwise).
TEST(BufferPoolTest, legacy_pool_switch) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.bounded_pool = false;
    auto queue = direct_queue(comm, conf);
    std::size_t received = 0;
    auto on_message = [&](auto envelope) {
        received++;
        EXPECT_EQ(envelope.message.size(), MESSAGE_SIZE);
    };
    comm.barrier();
    std::vector<int> const message(MESSAGE_SIZE, comm.rank_signed());
    for (std::size_t i = 0; i < MESSAGES_PER_RECEIVER; ++i) {
        for (int receiver = 0; receiver < comm.size_signed(); ++receiver) {
            queue.post_message_blocking(std::vector<int>(message), receiver, on_message);
        }
        queue.poll(on_message);
    }
    while (!queue.terminate(on_message)) {
    }
    EXPECT_GT(queue.num_free_buffers(), queue.num_allocated_buffers());
    EXPECT_EQ(global_sum(comm, received), MESSAGES_PER_RECEIVER * comm.size() * comm.size());
}

// Config::drop_shells on top of the legacy pool handling: dropping the moved-from shells alone keeps the pool
// bounded, although overflow flushes keep the destination's map entry.
TEST(BufferPoolTest, legacy_pool_drop_shells) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.bounded_pool = false;
    conf.drop_shells = true;
    auto queue = direct_queue(comm, conf);
    run_and_check(comm, queue);
    EXPECT_GE(queue.num_dropped_shells(), MESSAGES_PER_RECEIVER / 6);
}

// Config::drain_order: every order delivers everything and keeps the pool bounded.
TEST(BufferPoolTest, drain_order_rank) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.drain_order = briefkasten::DrainOrder::rank;
    auto queue = direct_queue(comm, conf);
    run_and_check(comm, queue);
    EXPECT_GE(queue.num_drains(), 1);
}

TEST(BufferPoolTest, drain_order_rotating) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.drain_order = briefkasten::DrainOrder::rotating;
    auto queue = direct_queue(comm, conf);
    run_and_check(comm, queue);
    EXPECT_GE(queue.num_drains(), 1);
}

TEST(BufferPoolTest, drain_order_rotating_grid) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.drain_order = briefkasten::DrainOrder::rotating;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}
