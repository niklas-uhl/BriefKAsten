#include <gtest/gtest.h>

#include <algorithm>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/communicator.hpp>
#include <message-queue/queue_builder.hpp>
#include <print>
#include <random>
#include "gmock/gmock.h"

/// Chunked interleaved alltoall using the message queue
TEST(BufferedQueueTest, alltoall) {
    using namespace ::testing;
    namespace kmp = kamping::params;
    kamping::Communicator<> comm;
    std::vector<int> data(1'000'000);
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, comm.size_signed() - 1);
    std::ranges::generate(data, [&]() { return distribution(generator); });
    auto queue = message_queue::BufferedMessageQueueBuilder<int>().build();
    queue.synchronous_mode();
    std::vector<int> received_data;
    auto on_message = [&](auto envelope) {
        received_data.insert_range(received_data.end(), envelope.message);
    };
    std::size_t messages_in_post = 0;
    for (auto& element : data) {
        queue.post_message_blocking(element, element, [&](auto envelope) {
            messages_in_post++;
	    on_message(std::move(envelope));
      });
    }
    std::size_t messages_in_terminate = 0;
    std::ignore = queue.terminate([&](auto envelope) {
        messages_in_terminate++;
        on_message(std::move(envelope));
    });
    std::println("Message received: in post: {}, in terminate: {}", messages_in_post, messages_in_terminate);
    EXPECT_THAT(received_data, Each(Eq(comm.rank())));
    auto total_receive_count = comm.allreduce_single(kmp::send_buf(received_data.size()), kmp::op(std::plus<>{}));
    EXPECT_EQ(total_receive_count, data.size() * comm.size());
}
