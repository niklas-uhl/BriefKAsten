#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <kamping/collectives/allreduce.hpp>
#include <kamping/communicator.hpp>

#include <cstddef>
#include <vector>

#include "briefkasten/aggregators.hpp"
#include "briefkasten/grid_indirection.hpp"
#include "briefkasten/indirection.hpp"
#include "briefkasten/queue_builder.hpp"

namespace {
// 1 KiB buffers: 256 ints. EnvelopeSerializationMerger adds two words (receiver, size) per message, so a message of
// 254 elements fills a buffer exactly and one of 255 does not fit even an empty buffer.
constexpr std::size_t THRESHOLD_BYTES = 1024;
constexpr std::size_t BUFFER_ELEMENTS = THRESHOLD_BYTES / sizeof(int);
constexpr std::size_t FITS_EXACTLY = BUFFER_ELEMENTS - 2;
const std::vector<std::size_t> MESSAGE_SIZES = {1, 100, FITS_EXACTLY, FITS_EXACTLY + 1, 2 * BUFFER_ELEMENTS,
                                                100 * BUFFER_ELEMENTS};

/// The message from `sender` of length `size`: [sender, size, 0, 1, ...], so the receiver can check it whole.
std::vector<int> make_message(int sender, std::size_t size) {
    std::vector<int> message(size);
    for (std::size_t i = 0; i < size; ++i) {
        message[i] = static_cast<int>(i);
    }
    if (size >= 2) {
        message[0] = sender;
        message[1] = static_cast<int>(size);
    }
    return message;
}

struct Outcome {
    std::size_t messages = 0;
    std::size_t elements = 0;
    std::size_t bad_messages = 0;
};

/// Every rank sends every message size to every rank (itself included), interleaving small and large ones, and
/// counts what arrives intact.
template <typename Queue>
Outcome run(kamping::Communicator<> const& comm, Queue& queue) {
    Outcome outcome;
    auto on_message = [&](auto envelope) {
        std::vector<int> message(envelope.message.begin(), envelope.message.end());
        outcome.messages++;
        outcome.elements += message.size();
        bool ok = true;
        if (message.size() >= 2) {
            ok = message[1] == static_cast<int>(message.size()) && message[0] >= 0 && message[0] < comm.size_signed();
            for (std::size_t i = 2; ok && i < message.size(); ++i) {
                ok = message[i] == static_cast<int>(i);
            }
        }
        outcome.bad_messages += ok ? 0 : 1;
    };
    for (std::size_t round = 0; round < 3; ++round) {
        for (std::size_t size : MESSAGE_SIZES) {
            for (int receiver = 0; receiver < comm.size_signed(); ++receiver) {
                queue.post_message_blocking(make_message(comm.rank_signed(), size), receiver, on_message);
                queue.poll(on_message);
            }
        }
    }
    while (!queue.terminate(on_message)) {
    }
    return outcome;
}

std::size_t global_sum(kamping::Communicator<> const& comm, std::size_t value) {
    return comm.allreduce_single(kamping::params::send_buf(value), kamping::params::op(std::plus<>{}));
}

void check(kamping::Communicator<> const& comm, Outcome const& outcome) {
    std::size_t elements_per_round = 0;
    for (std::size_t size : MESSAGE_SIZES) {
        elements_per_round += size;
    }
    auto const p = comm.size();
    EXPECT_EQ(global_sum(comm, outcome.messages), 3 * MESSAGE_SIZES.size() * p * p);
    EXPECT_EQ(global_sum(comm, outcome.elements), 3 * elements_per_round * p * p);
    EXPECT_EQ(global_sum(comm, outcome.bad_messages), 0U);
}

briefkasten::Config small_buffers() {
    briefkasten::Config conf;
    conf.local_threshold_bytes = THRESHOLD_BYTES;
    return conf;
}

auto direct_queue(kamping::Communicator<> const& comm, briefkasten::Config conf) {
    return briefkasten::BufferedMessageQueueBuilder<int>(comm.mpi_communicator(), conf)
        .with_merger(briefkasten::aggregation::EnvelopeSerializationMerger{})
        .with_splitter(briefkasten::aggregation::EnvelopeSerializationSplitter<int>{})
        .build();
}
}  // namespace

/// A message that does not fit an empty buffer must still be delivered, whole and once.
TEST(LargeMessageTest, direct) {
    kamping::Communicator<> comm;
    auto queue = direct_queue(comm, small_buffers());
    check(comm, run(comm, queue));
}

TEST(LargeMessageTest, direct_with_flow_control) {
    kamping::Communicator<> comm;
    auto conf = small_buffers();
    conf.num_credit_buffers = 2;
    auto queue = direct_queue(comm, conf);
    check(comm, run(comm, queue));
}

/// Through the grid: a proxy receives the oversized message and has to forward it the same way.
TEST(LargeMessageTest, grid) {
    kamping::Communicator<> comm;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, small_buffers()),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    check(comm, run(comm, queue));
}

TEST(LargeMessageTest, grid_without_flow_control) {
    kamping::Communicator<> comm;
    auto conf = small_buffers();
    conf.num_credit_buffers = 0;
    briefkasten::IndirectionAdapter queue{direct_queue(comm, conf),
                                          briefkasten::GridIndirectionScheme{comm.mpi_communicator()}};
    check(comm, run(comm, queue));
}
