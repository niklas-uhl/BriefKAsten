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

namespace message_queue::internal {

struct MessageCounter {
    size_t send;
    size_t receive;
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

    void start_message_counting() {
        if (reduce_req_ == MPI_REQUEST_NULL) {
            global_ = local_;
            MPI_Iallreduce(MPI_IN_PLACE, &global_, 2, kamping::mpi_datatype<std::size_t>(), MPI_SUM, comm_,
                           &reduce_req_);
        }
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
        bool terminated = global_ == previous_global_ && global_.send == global_.receive;
        if (!terminated) {
            // store for double counting
            previous_global_ = global_;
            global_ = {.send = 0, .receive = 0};
        }
        return terminated;
    }

private:
    MPI_Comm comm_;
    MPI_Request reduce_req_ = MPI_REQUEST_NULL;
    MessageCounter local_{.send = 0, .receive = 0};
    MessageCounter global_{.send = 0, .receive = 0};
    MessageCounter previous_global_{.send = std::numeric_limits<std::size_t>::max(),
                                    .receive = std::numeric_limits<std::size_t>::max() - 1};
};

}  // namespace message_queue::internal
