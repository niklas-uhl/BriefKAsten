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

// Experiment switches Config::separate_forwarding and Config::exact_redirect_limit, alone and together, with the
// default credit window and with one buffer per peer: everything arrives, and the reserve stays within the ceiling
// for the switches in use (also checked at every acquisition by a KASSERT).
void run_grid(bool separate_forwarding,
              bool exact_redirect_limit,
              std::size_t num_credit_buffers,
              bool drop_shells = false) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = num_credit_buffers;
    conf.separate_forwarding = separate_forwarding;
    conf.exact_redirect_limit = exact_redirect_limit;
    conf.drop_shells = drop_shells;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
    if (separate_forwarding) {
        EXPECT_EQ(queue.queue().num_reserve_waits(), 0);
    }
}

TEST(BufferPoolTest, grid_separate_forwarding) {
    run_grid(true, false, briefkasten::DEFAULT_NUM_CREDIT_BUFFERS);
}

TEST(BufferPoolTest, grid_separate_forwarding_small_credit_window) {
    run_grid(true, false, 1);
}

TEST(BufferPoolTest, grid_exact_redirect_limit) {
    run_grid(false, true, briefkasten::DEFAULT_NUM_CREDIT_BUFFERS);
}

TEST(BufferPoolTest, grid_exact_redirect_limit_small_credit_window) {
    run_grid(false, true, 1);
}

TEST(BufferPoolTest, grid_separate_forwarding_exact_redirect_limit) {
    run_grid(true, true, briefkasten::DEFAULT_NUM_CREDIT_BUFFERS);
}

TEST(BufferPoolTest, grid_separate_forwarding_exact_redirect_limit_small_credit_window) {
    run_grid(true, true, 1);
}

// KaCCv2 runs with Config::drop_shells (the candidate pool fix): the moved-from shells of either kind of buffer are
// dropped, never recycled
TEST(BufferPoolTest, grid_separate_forwarding_exact_redirect_limit_drop_shells) {
    run_grid(true, true, briefkasten::DEFAULT_NUM_CREDIT_BUFFERS, /*drop_shells=*/true);
}

TEST(BufferPoolTest, grid_separate_forwarding_exact_redirect_limit_drop_shells_small_credit_window) {
    run_grid(true, true, 1, /*drop_shells=*/true);
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

// Every switch at the behaviour of 5664f7f (KaCCv2's base) except the leak, which drop_shells removes: still
// delivers everything and keeps the pool bounded.
TEST(BufferPoolTest, old_base_switches_with_drop_shells) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.bounded_pool = false;
    conf.drop_shells = true;
    conf.idle_flush = briefkasten::IdleFlush::off;
    conf.allow_large_messages = false;
    conf.poll_while_waiting = false;
    conf.half_window_oversize_pass = false;
    auto queue = direct_queue(comm, conf);
    run_and_check(comm, queue);
}

// The candidate: bounded_pool (with #30's reserve rules) and drop_shells instead of #29's erase.
TEST(BufferPoolTest, drop_shells) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.drop_shells = true;
    auto queue = direct_queue(comm, conf);
    run_and_check(comm, queue);
    EXPECT_GE(queue.num_dropped_shells(), MESSAGES_PER_RECEIVER / 6);
}

// drop_shells under indirection: a proxy's handler may post into a destination whose entry still holds the shell
// of a buffer just flushed by the overflowing post; that post must take a pool buffer, not grow the shell.
TEST(BufferPoolTest, drop_shells_grid) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.drop_shells = true;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}

TEST(BufferPoolTest, drop_shells_grid_small_credit_window) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.drop_shells = true;
    conf.num_credit_buffers = 2;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    run_and_check(comm, queue);
}

namespace {
// Config::credit_budget_buffers: windows start at two buffers and grow within the budget. Everything arrives, the
// pool stays within its ceiling, and the credit outstanding on all inbound links never exceeds the budget.
template <typename MakeQueue>
void run_budget(std::size_t num_credit_buffers, std::size_t credit_budget_buffers, MakeQueue make_queue) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = num_credit_buffers;
    conf.credit_budget_buffers = credit_budget_buffers;
    auto queue = make_queue(comm, conf);
    run_and_check(comm, queue);
    auto const& base = [&]() -> auto const& {
        if constexpr (requires { queue.queue(); }) {
            return queue.queue();
        } else {
            return queue;
        }
    }();
    EXPECT_GT(base.credit_budget_elements(), 0U);
    EXPECT_LE(base.peak_inbound_credit(), base.credit_budget_elements());
    // a budget of two buffers per peer is all initial windows: nothing can grow
    if (comm.size() > 1 && credit_budget_buffers > 2) {
        EXPECT_GT(global_sum(comm, base.num_window_grows()), 0U);
    }
    if (credit_budget_buffers == 2) {
        EXPECT_EQ(base.num_window_grows(), 0U);
    }
}

auto grid_queue(kamping::Communicator<> const& comm, briefkasten::Config const& conf) {
    return briefkasten::IndirectionAdapter{direct_queue(comm, conf),
                                           briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
}
}  // namespace

TEST(BufferPoolTest, direct_credit_budget) {
    run_budget(briefkasten::DEFAULT_NUM_CREDIT_BUFFERS, 4, direct_queue);
}

TEST(BufferPoolTest, grid_credit_budget) {
    run_budget(briefkasten::DEFAULT_NUM_CREDIT_BUFFERS, 4, grid_queue);
}

// the smallest budget: every link stays at its initial window of two buffers
TEST(BufferPoolTest, grid_credit_budget_minimal) {
    run_budget(briefkasten::DEFAULT_NUM_CREDIT_BUFFERS, 2, grid_queue);
}

// Config::num_delivery_credit_buffers: links that are only delivered over get a larger window than the proxied ones.
// Everything arrives and the pool stays within the ceiling of the (proxied) window.
TEST(BufferPoolTest, grid_delivery_window) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = 2;
    conf.num_delivery_credit_buffers = 8;
    auto queue = grid_queue(comm, conf);
    run_and_check(comm, queue);
}

// Config::feeder_first: withholding from the feeders of the congestion first must not lose anything or break the
// ceiling, also with the smallest window (most withholds).
TEST(BufferPoolTest, grid_feeder_first) {
    kamping::Communicator<> comm;
    auto conf = small_buffers(/*flow_control=*/true);
    conf.num_credit_buffers = 2;
    conf.feeder_first = true;
    auto queue = grid_queue(comm, conf);
    run_and_check(comm, queue);
}
