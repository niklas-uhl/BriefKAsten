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

#include "./detail/definitions.hpp"

namespace message_queue {
class NoopIndirectionScheme {
public:
    NoopIndirectionScheme(MPI_Comm comm) {
        MPI_Comm_rank(comm, &my_rank_);
        MPI_Comm_size(comm, &my_size_);
    }

    [[nodiscard]] PEID next_hop(PEID /* sender */, PEID receiver) const {
        return receiver;
    }
    [[nodiscard]] bool should_redirect(PEID /*sender*/, PEID receiver) const {
        return receiver != rank();
    }

    [[nodiscard]] PEID group_size() const {
        auto placeholder = size();
        return placeholder;
    }

    [[nodiscard]] PEID num_groups() const {
        return 1;
    }

private:
    [[nodiscard]] int rank() const {
        return my_rank_;
    }
  
    [[nodiscard]] int size() const {
        return my_size_;
    }

    int my_rank_ = 0;
    int my_size_ = 0;
};
}  // namespace message_queue
