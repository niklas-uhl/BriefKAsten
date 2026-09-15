// Copyright (c) 2021-2025 Tim Niklas Uhl
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

#pragma once

#include <mpi.h>
#include <cstddef>
#include <kamping/mpi_datatype.hpp>
#include <limits>

namespace briefkasten::internal {

struct MessageCounter {
    size_t send;
    size_t receive;
    /// Payload accepted into an aggregation buffer but not yet handed to MPI.
    ///
    /// send/receive are counted per PACKET, at flush and at arrival. That makes a relayed message
    /// sitting in a proxy's buffer invisible: the packet that carried it was sent once and received
    /// once, so the counts balance while the data is still undelivered. Termination would fire and
    /// the message would be lost. The old defence was to force-flush every relay buffer on every
    /// termination attempt, which is correct but fragments the traffic -- 64% of relay packets at
    /// p=608, at 5% fill (see notes/takeover_relay_backpressure.md).
    ///
    /// Counting the outstanding buffer contents instead makes the imbalance explicit, so
    /// termination refuses on its own and flushing becomes a question of progress rather than of
    /// correctness. Deliberately read from BufferedMessageQueue's existing global_buffer_size_
    /// rather than tracked as a parallel per-message counter: that accounting already subtracts
    /// the PRE-cleanup buffer size on flush, so anything a BufferCleaner discards is handled for
    /// free. A separate merge-time counter would have needed every cleaner to report its
    /// discards, and would have gone wrong silently when one did not.
    size_t pending = 0;
    auto operator<=>(const MessageCounter&) const = default;
};

class TerminationCounter {
public:
    TerminationCounter(MPI_Comm comm) : comm_(comm) {}

    void track_send() {
        local_.send++;
    }

    void track_receive() {
        local_.receive++;
    }

    /// Snapshot of the locally tracked send/receive counts. Used to fold a sibling queue's counts into a single
    /// joint termination round (see IndirectionAdapter), so the whole multi-hop system is counted in one allreduce.
    [[nodiscard]] MessageCounter local_counts() const {
        return local_;
    }

    void start_message_counting(MessageCounter additional = {.send = 0, .receive = 0}) {
        if (reduce_req_ == MPI_REQUEST_NULL) {
            global_ = {.send = local_.send + additional.send,
                       .receive = local_.receive + additional.receive,
                       .pending = additional.pending};
            MPI_Iallreduce(MPI_IN_PLACE, &global_, 3, kamping::mpi_datatype<std::size_t>(), MPI_SUM, comm_,
                           &reduce_req_);
            num_termination_rounds_++;
        }
    }

    [[nodiscard]] std::size_t num_termination_rounds() const {
        return num_termination_rounds_;
    }

    [[nodiscard]] bool message_counting_finished() {
        if (reduce_req_ == MPI_REQUEST_NULL) {
            return true;
        }
        int reduce_finished = 0;
        MPI_Test(&reduce_req_, &reduce_finished, MPI_STATUS_IGNORE);
        return static_cast<bool>(reduce_finished);
    }

    [[nodiscard]] bool terminated() {
        // `pending == 0` is required in addition to the balance: a payload still sitting in some
        // rank's aggregation buffer is neither sent nor received, so the balance alone cannot see it.
        bool terminated =
            global_ == previous_global_ && global_.send == global_.receive && global_.pending == 0;
        if (!terminated) {
            // store for double counting
            previous_global_ = global_;
            global_ = {.send = 0, .receive = 0, .pending = 0};
        }
        return terminated;
    }

private:
    MPI_Comm comm_;
    MPI_Request reduce_req_ = MPI_REQUEST_NULL;
    MessageCounter local_{.send = 0, .receive = 0};
    std::size_t num_termination_rounds_ = 0;
    MessageCounter global_{.send = 0, .receive = 0};
    MessageCounter previous_global_{.send = std::numeric_limits<std::size_t>::max(),
                                    .receive = std::numeric_limits<std::size_t>::max() - 1};
};

}  // namespace briefkasten::internal
