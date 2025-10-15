#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/communicator.hpp>
#include <kamping/types/tuple.hpp>
#include <print>

#include <algorithm>
#include <random>

#include "briefkasten/aggregators.hpp"
#include "briefkasten/buffered_queue.hpp"
#include "briefkasten/grid_indirection.hpp"
#include "briefkasten/indirection.hpp"
#include "briefkasten/queue_builder.hpp"

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
    auto queue = briefkasten::BufferedMessageQueueBuilder<int>().build();
    queue.synchronous_mode();

    // communication
    std::vector<int> received_data;
    auto on_message = [&](auto envelope) {
        received_data.insert(received_data.end(), envelope.message.begin(), envelope.message.end());
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

/// Chunked interleaved alltoall using the message queue
TEST(BufferedQueueTest, alltoall_tuple) {
    using namespace ::testing;
    namespace kmp = kamping::params;
    kamping::Communicator<> comm;
    // generate data
    std::vector<std::tuple<int, int>> data(NUM_LOCAL_ELEMENTS);
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, comm.size_signed() - 1);
    std::ranges::generate(data, [&]() { return std::tuple{distribution(generator), comm.rank_signed()}; });

    // init queue
    auto queue = briefkasten::BufferedMessageQueueBuilder<std::tuple<int, int>>().build();
    queue.synchronous_mode();

    // communication
    std::vector<std::tuple<int, int>> received_data;
    auto on_message = [&](auto envelope) {
        received_data.insert(received_data.end(), envelope.message.begin(), envelope.message.end());
    };
    for (auto& element : data) {
        queue.post_message_blocking(element, std::get<0>(element), on_message);
    }
    std::ignore = queue.terminate(on_message);

    // tests
    EXPECT_THAT(received_data, Each(FieldsAre(Eq(comm.rank()), A<int>())));
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
    briefkasten::IndirectionAdapter queue{
        briefkasten::BufferedMessageQueueBuilder<int>()
            // we have to use splitters and merges which encode receiver information and size,
            // so that indirection works.
            .with_merger(briefkasten::aggregation::EnvelopeSerializationMerger{})
            .with_splitter(briefkasten::aggregation::EnvelopeSerializationSplitter<int>{})
            .build(),
        briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    queue.synchronous_mode();

    // communication
    std::vector<int> received_data;
    auto on_message = [&](auto envelope) {
        received_data.insert(received_data.end(), envelope.message.begin(), envelope.message.end());
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

TEST(BufferedQueueTest, alltoall_indirect_tuple) {
    using namespace ::testing;
    namespace kmp = kamping::params;
    kamping::Communicator<> comm;

    // generate data
    std::vector<std::tuple<int, int>> data(NUM_LOCAL_ELEMENTS);
    // std::vector<std::tuple<int, int>> data(2);
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, comm.size_signed() - 1);
    std::ranges::generate(data, [&]() { return std::tuple{distribution(generator), comm.rank_signed()}; });

    // queue setup
    briefkasten::IndirectionAdapter queue{
        briefkasten::BufferedMessageQueueBuilder<std::tuple<int, int>>()
            .with_buffer_type<int>()
            // we have to use splitters and merges which encode receiver information and size,
            // so that indirection works.
            .with_merger(briefkasten::aggregation::EnvelopeSerializationMerger{})
            .with_splitter(briefkasten::aggregation::EnvelopeSerializationSplitter<std::tuple<int, int>>{})
	.with_buffer_cleaner([&](auto buf, int rank){
	  // std::println("Sending {} to {}", buf, rank);
})
            .build(),
        briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    queue.synchronous_mode();

    // communication
    std::vector<std::tuple<int, int>> received_data;
    auto on_message = [&](auto envelope) {
        received_data.insert(received_data.end(), envelope.message.begin(), envelope.message.end());
    };
    for (auto& element : data) {
        queue.post_message_blocking(element, std::get<0>(element), on_message);
    }
    std::ignore = queue.terminate(on_message);

    // tests
    EXPECT_THAT(received_data, Each(FieldsAre(Eq(comm.rank()), A<int>())));
    auto total_receive_count = comm.allreduce_single(kmp::send_buf(received_data.size()), kmp::op(std::plus<>{}));
    EXPECT_EQ(total_receive_count, data.size() * comm.size());
}
