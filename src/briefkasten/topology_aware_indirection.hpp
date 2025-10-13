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

#include <kamping/communicator.hpp>

#include "./detail/definitions.hpp"

namespace briefkasten {
class TopologyAwareIndirectionScheme {
public:
    TopologyAwareIndirectionScheme(MPI_Comm comm) : comm_(comm) {
        auto intra_node_comm = comm_.split_to_shared_memory();
    }
    [[nodiscard]] PEID next_hop(PEID /* sender */, PEID receiver) const {
        return receiver;
    }

    [[nodiscard]] bool should_redirect(PEID /* sender */, PEID receiver) const {
        return receiver != comm_.size_signed();
    }

private:
    kamping::Communicator<> comm_;
};
}  // namespace briefkasten
