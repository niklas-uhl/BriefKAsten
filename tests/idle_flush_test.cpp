#include <gtest/gtest.h>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/collectives/barrier.hpp>
#include <kamping/communicator.hpp>

#include <cstddef>
#include <deque>
#include <functional>
#include <random>

#include "briefkasten/aggregators.hpp"
#include "briefkasten/buffered_queue.hpp"
#include "briefkasten/grid_indirection.hpp"
#include "briefkasten/indirection.hpp"
#include "briefkasten/queue_builder.hpp"

// NOLINTBEGIN(*-magic-numbers)
namespace {
constexpr std::size_t NUM_CHAINS = 200;
constexpr int CHAIN_LENGTH = 30;

auto make_flat_queue(briefkasten::Config const& conf) {
    return briefkasten::BufferedMessageQueueBuilder<int>(conf)
        .with_merger(briefkasten::aggregation::EnvelopeSerializationMerger{})
        .with_splitter(briefkasten::aggregation::EnvelopeSerializationSplitter<int>{})
        .build();
}

auto make_grid_queue(briefkasten::Config const& conf, kamping::Communicator<> const& comm) {
    return briefkasten::IndirectionAdapter{make_flat_queue(conf),
                                           briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
}

/// Ruler-chasing-shaped workload: every rank starts num_chains tokens, each hops CHAIN_LENGTH times to a random
/// rank, one message in flight per chain. Per-peer traffic stays far below the buffer threshold, so buffers only
/// leave through the idle flush or the termination drain. Returns the number of chains completed globally.
std::size_t run_chains(auto& queue, kamping::Communicator<> const& comm, std::size_t num_chains = NUM_CHAINS) {
    std::default_random_engine generator{static_cast<std::default_random_engine::result_type>(comm.rank_signed())};
    std::uniform_int_distribution<int> rank_distribution(0, comm.size_signed() - 1);
    std::deque<int> work(num_chains, CHAIN_LENGTH);
    std::size_t completed = 0;
    auto on_message = [&](auto envelope) {
        for (int remaining : envelope.message) {
            work.push_back(remaining);
        }
    };
    do {  // NOLINT(*-avoid-do-while)
        while (!work.empty()) {
            int remaining = work.front();
            work.pop_front();
            if (remaining == 0) {
                completed++;
            } else {
                queue.post_message_blocking(remaining - 1, rank_distribution(generator), on_message);
            }
            queue.poll_throttled(on_message);
        }
    } while (!queue.terminate(on_message));
    return comm.allreduce_single(kamping::send_buf(completed), kamping::op(std::plus<>{}));
}

void check_counters(auto const& queue, briefkasten::IdleFlush mode) {
    switch (mode) {
        case briefkasten::IdleFlush::off:
            EXPECT_EQ(queue.num_idle_flushes(), 0);
            EXPECT_EQ(queue.num_idle_flush_skips(), 0);
            break;
        case briefkasten::IdleFlush::all:
            // the first terminate() call finds the initial posts still buffered, far below the threshold
            EXPECT_GT(queue.num_idle_flushes(), 0);
            EXPECT_EQ(queue.num_idle_flush_skips(), 0);
            break;
        case briefkasten::IdleFlush::stale:
            // the first attempt sees every buffer for the first time, which counts as grown
            EXPECT_GT(queue.num_idle_flush_skips(), 0);
            break;
    }
}

class IdleFlushTest : public ::testing::TestWithParam<briefkasten::IdleFlush> {};
}  // namespace

TEST_P(IdleFlushTest, flat_chains_complete) {
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    conf.idle_flush = GetParam();
    auto queue = make_flat_queue(conf);
    EXPECT_EQ(run_chains(queue, comm), NUM_CHAINS * comm.size());
    check_counters(queue, GetParam());
    comm.barrier();
}

TEST_P(IdleFlushTest, grid_chains_complete) {
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    conf.idle_flush = GetParam();
    auto queue = make_grid_queue(conf, comm);
    EXPECT_EQ(queue.idle_flush(), GetParam());  // an explicit choice survives the adapter's default
    EXPECT_EQ(run_chains(queue, comm), NUM_CHAINS * comm.size());
    check_counters(queue.queue(), GetParam());
    comm.barrier();
}

TEST_P(IdleFlushTest, grid_chains_complete_under_credit_pressure) {
    // tiny buffers and a two-buffer credit window (a one-buffer window hangs even with IdleFlush::off): idle
    // flushes routinely find no credit, and a proxy forwards
    // into buffers it shares with its own posts. Parking those partially filled buffers instead of leaving them
    // in place would let the proxy overdraw the pool past the credit-derived ceiling (KASSERT in acquire_buffer).
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    conf.idle_flush = GetParam();
    conf.local_threshold_bytes = 64;
    conf.num_credit_buffers = 2;
    auto queue = make_grid_queue(conf, comm);
    EXPECT_EQ(run_chains(queue, comm, 2000), 2000 * comm.size());
    comm.barrier();
}

TEST_P(IdleFlushTest, flat_chains_complete_with_one_send_slot) {
    // one request slot: every idle flush with more than one non-empty buffer runs out of slots and stops, so
    // the resume point is exercised on every attempt, with and without round robin
    for (bool round_robin : {true, false}) {
        kamping::Communicator<> comm;
        briefkasten::Config conf;
        conf.idle_flush = GetParam();
        conf.idle_flush_round_robin = round_robin;
        conf.num_request_slots = 1;
        auto queue = make_flat_queue(conf);
        EXPECT_EQ(run_chains(queue, comm), NUM_CHAINS * comm.size()) << "round_robin=" << round_robin;
        if (GetParam() == briefkasten::IdleFlush::all && comm.size() > 1) {
            EXPECT_GT(queue.num_idle_flush_stops(), 0) << "round_robin=" << round_robin;
        }
        comm.barrier();
    }
}

TEST_P(IdleFlushTest, grid_chains_complete_with_one_send_slot) {
    for (bool round_robin : {true, false}) {
        kamping::Communicator<> comm;
        briefkasten::Config conf;
        conf.idle_flush = GetParam();
        conf.idle_flush_round_robin = round_robin;
        conf.num_request_slots = 1;
        auto queue = make_grid_queue(conf, comm);
        EXPECT_EQ(run_chains(queue, comm), NUM_CHAINS * comm.size()) << "round_robin=" << round_robin;
        comm.barrier();
    }
}

TEST_P(IdleFlushTest, flat_chains_complete_with_send_backlog) {
    // an explicit backlog survives flow control, so flushes queue behind the slots instead of stopping
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    conf.idle_flush = GetParam();
    conf.send_backlog_capacity = comm.size();
    auto queue = make_flat_queue(conf);
    EXPECT_EQ(run_chains(queue, comm), NUM_CHAINS * comm.size());
    comm.barrier();
}

TEST_P(IdleFlushTest, flat_chains_complete_without_flow_control) {
    // without credits a flush can fail for lack of a send slot; the idle flush must stop, not spin
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    conf.idle_flush = GetParam();
    conf.num_credit_buffers = 0;
    conf.num_request_slots = 1;
    auto queue = make_flat_queue(conf);
    EXPECT_EQ(run_chains(queue, comm), NUM_CHAINS * comm.size());
    comm.barrier();
}

INSTANTIATE_TEST_SUITE_P(Modes,
                         IdleFlushTest,
                         ::testing::Values(briefkasten::IdleFlush::off,
                                           briefkasten::IdleFlush::all,
                                           briefkasten::IdleFlush::stale));

TEST(IdleFlushDefaults, flat_flushes_all_grid_flushes_nothing) {
    kamping::Communicator<> comm;
    briefkasten::Config conf;
    auto flat = make_flat_queue(conf);
    EXPECT_EQ(flat.idle_flush(), briefkasten::IdleFlush::all);
    auto grid = make_grid_queue(conf, comm);
    EXPECT_EQ(grid.idle_flush(), briefkasten::IdleFlush::off);
}
// NOLINTEND(*-magic-numbers)
