// Copyright (c) 2021-2023 Tim Niklas Uhl
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <CLI/CLI.hpp>
#include <message-queue/buffered_queue.hpp>
#include <print>
#include <random>
#include "message-queue/queue_builder.hpp"

auto main(int argc, char* argv[]) -> int {
    MPI_Init(nullptr, nullptr);
    CLI::App app;
    app.option_defaults()->always_capture_default();

    size_t number_of_messages = 5;
    app.add_option("--number_of_messages", number_of_messages, "The number of messages to send from each PE");

    CLI11_PARSE(app, argc, argv);

    auto printing_cleaner = [](auto& buf, message_queue::PEID receiver) {
        std::print("Preparing buffer {} to {}.\n", buf, receiver);
    };
    {
        auto queue1 =
            message_queue::BufferedMessageQueueBuilder<int>().with_buffer_cleaner(std::move(printing_cleaner)).build();

        queue1.synchronous_mode();
        MPI_Comm other_comm;
        MPI_Comm_dup(MPI_COMM_WORLD, &other_comm);
        auto queue2 = message_queue::BufferedMessageQueueBuilder<int>({}, other_comm)
                          .with_buffer_cleaner(std::move(printing_cleaner))
                          .build();

        queue2.synchronous_mode();
        int rank, size;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);
        std::mt19937 gen;
        std::uniform_int_distribution<int> dist(0, size - 1);
        for (std::size_t i = 0; i < number_of_messages; ++i) {
            int val = dist(gen);
            queue1.post_message(1, val);
            queue2.post_message(2, val);
        }
        auto _ = queue2.terminate([&](message_queue::Envelope<int> auto envelope) {
            std::print("Message {} from {} arrived.\n", envelope.message, envelope.sender);
        });

        _ = queue1.terminate([&](message_queue::Envelope<int> auto envelope) {
            std::print("Message {} from {} arrived.", envelope.message, envelope.sender);
        });
    }
    MPI_Finalize();
    return 0;
}
