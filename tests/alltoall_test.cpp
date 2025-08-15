#include <gtest/gtest.h>

#include <algorithm>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/communicator.hpp>
#include <message-queue/queue_builder.hpp>
#include <random>
#include "gmock/gmock.h"
#include "message-queue/aggregators.hpp"
#include "message-queue/buffered_queue.hpp"
#include "message-queue/grid_indirection.hpp"
#include "message-queue/indirection.hpp"

constexpr std::size_t NUM_LOCAL_ELEMENTS = 1'000'000;

/// Chunked interleaved alltoall using the message queue
TEST(BufferedQueueTest, alltoall) {
    using namespace ::testing;
    namespace kmp = kamping::params;
    kamping::Communicator<> comm;
    // generate data
    std::vector<int> data(NUM_LOCAL_ELEMENTS);
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, comm.size_signed() - 1);
    std::ranges::generate(data, [&]() { return distribution(generator); });

    // init queue
    auto queue = message_queue::BufferedMessageQueueBuilder<int>().build();
    queue.synchronous_mode();

    // communication
    std::vector<int> received_data;
    auto on_message = [&](auto envelope) {
        received_data.insert_range(received_data.end(), envelope.message);
    };
    for (auto& element : data) {
        queue.post_message_blocking(element, element, on_message);
    }
    std::ignore = queue.terminate(on_message);

    // tests
    EXPECT_THAT(received_data, Each(Eq(comm.rank())));
    auto total_receive_count = comm.allreduce_single(kmp::send_buf(received_data.size()), kmp::op(std::plus<>{}));
    EXPECT_EQ(total_receive_count, data.size() * comm.size());
}

TEST(BufferedQueueTest, alltoall_indirect) {
    using namespace ::testing;
    namespace kmp = kamping::params;
    kamping::Communicator<> comm;

    // generate data
    std::vector<int> data(NUM_LOCAL_ELEMENTS);
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, comm.size_signed() - 1);
    std::ranges::generate(data, [&]() { return distribution(generator); });

    // queue setup
    message_queue::IndirectionAdapter queue{
        message_queue::BufferedMessageQueueBuilder<int>()
            .with_merger(message_queue::aggregation::EnvelopeSerializationMerger{})
            .with_splitter(message_queue::aggregation::EnvelopeSerializationSplitter<int>{})
            .build(),
        message_queue::GridIndirectionScheme{comm.mpi_communicator()}};
    queue.synchronous_mode();

    // communication
    std::vector<int> received_data;
    auto on_message = [&](auto envelope) {
        received_data.insert_range(received_data.end(), envelope.message);
    };
    for (auto& element : data) {
        queue.post_message_blocking(element, element, on_message);
    }
    std::ignore = queue.terminate(on_message);

    // tests
    EXPECT_THAT(received_data, Each(Eq(comm.rank())));
    auto total_receive_count = comm.allreduce_single(kmp::send_buf(received_data.size()), kmp::op(std::plus<>{}));
    EXPECT_EQ(total_receive_count, data.size() * comm.size());
}
