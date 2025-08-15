#include <gtest/gtest.h>
#include <kamping/collectives/barrier.hpp>
#include <kamping/communicator.hpp>

#include <deque>
#include <print>
#include <random>
#include <vector>
#include "message-queue/aggregators.hpp"
#include "message-queue/grid_indirection.hpp"
#include "message-queue/indirection.hpp"
#include "message-queue/queue_builder.hpp"

constexpr std::size_t INITIAL_TASKS = 1000;

// NOLINTBEGIN(*-magic-numbers)
TEST(BufferedQueueTest, workloop) {
    // each rank generates a fixed number of tasks, consisting of integer ranges:
    // the first value is the time-to-live, the second value is the number of hops, followed by the list of ranks this
    // task has been forwarded to. For each task, each rank draws a random branching factor r between 1 and 4, appends
    // its rank to the task and forwards it to r random ranks. When the time-to-live reaches zero, no new tasks are
    // spawned.

    kamping::Communicator<> comm;
    std::deque<std::vector<int>> tasks;
    std::default_random_engine generator{static_cast<std::default_random_engine::result_type>(comm.rank_signed())};
    std::uniform_int_distribution<int> distribution(1, 4);
    std::uniform_int_distribution<int> ttl_distribution(5, 10);
    std::uniform_int_distribution<int> rank_distribution(0, comm.size_signed() - 1);
    // Generate initial tasks
    for (std::size_t i = 0; i < INITIAL_TASKS; ++i) {
        std::vector<int> task{ttl_distribution(generator), 0};
        tasks.push_back(std::move(task));
    }
    auto queue = message_queue::BufferedMessageQueueBuilder<int>()
                     .with_merger(message_queue::aggregation::SentinelMerger<int>(-1))
                     .with_splitter(message_queue::aggregation::SentinelSplitter<int>(-1))
                     .build();
    auto on_message = [&](auto envelope) {
        auto task = std::move(envelope.message);
        tasks.push_back(std::vector(task.begin(), task.end()));
    };
    do {  // NOLINT(*-avoid-do-while)
        while (!tasks.empty()) {
            auto task = std::vector(tasks.front().begin(), tasks.front().end());
            tasks.pop_front();
            int ttl = task.at(0);
            if (ttl > 0) {
                task[0]--;                           // Decrease time-to-live
                task[1]++;                           // count hops
                task.push_back(comm.rank_signed());  // Append rank to task
                int branching_factor = distribution(generator);
                for (int i = 0; i < branching_factor; ++i) {
                    message_queue::PEID receiver = comm.rank_signed();
                    queue.post_message_blocking(std::ranges::ref_view(task), receiver, on_message);
                }
            } else {
                // task is done, check if num hops matches trace.
                EXPECT_EQ(task[1], task.size() - 2);
            }
        }
    } while (!queue.terminate(on_message));
}

TEST(BufferedQueueTest, workloop_indirect) {
    // each rank generates a fixed number of tasks, consisting of integer ranges:
    // the first value is the time-to-live, the second value is the number of hops, followed by the list of ranks this
    // task has been forwarded to. For each task, each rank draws a random branching factor r between 1 and 4, appends
    // its rank to the task and forwards it to r random ranks. When the time-to-live reaches zero, no new tasks are
    // spawned.

    kamping::Communicator<> comm;
    std::deque<std::vector<int>> tasks;
    std::default_random_engine generator{static_cast<std::default_random_engine::result_type>(comm.rank_signed())};
    std::uniform_int_distribution<int> distribution(1, 4);
    std::uniform_int_distribution<int> ttl_distribution(5, 10);
    std::uniform_int_distribution<int> rank_distribution(0, comm.size_signed() - 1);
    // Generate initial tasks
    for (std::size_t i = 0; i < INITIAL_TASKS; ++i) {
        std::vector<int> task{ttl_distribution(generator), 0};
        tasks.push_back(std::move(task));
    }
    message_queue::IndirectionAdapter queue{
        message_queue::BufferedMessageQueueBuilder<int>()
            .with_merger(message_queue::aggregation::EnvelopeSerializationMerger{})
            .with_splitter(message_queue::aggregation::EnvelopeSerializationSplitter<int>{})
            .build(),
        message_queue::GridIndirectionScheme{comm.mpi_communicator()}};
    auto on_message = [&](auto envelope) {
        auto task = std::move(envelope.message);
        tasks.push_back(std::vector(task.begin(), task.end()));
    };
    do {  // NOLINT(*-avoid-do-while)
        while (!tasks.empty()) {
            auto task = std::vector(tasks.front().begin(), tasks.front().end());
            tasks.pop_front();
            int ttl = task.at(0);
            if (ttl > 0) {
                task[0]--;                           // Decrease time-to-live
                task[1]++;                           // count hops
                task.push_back(comm.rank_signed());  // Append rank to task
                int branching_factor = distribution(generator);
                for (int i = 0; i < branching_factor; ++i) {
                    message_queue::PEID receiver = comm.rank_signed();
                    queue.post_message_blocking(std::ranges::ref_view(task), receiver, on_message);
                }
            } else {
                // task is done, check if num hops matches trace.
                EXPECT_EQ(task[1], task.size() - 2);
            }
        }
    } while (!queue.terminate(on_message));
}
// NOLINTEND(*-magic-numbers)
