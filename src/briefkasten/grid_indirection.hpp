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
#include <cmath>
#include <cstddef>

#include <kassert/kassert.hpp>

#include "./detail/definitions.hpp"

namespace briefkasten {

class GridIndirectionScheme {
public:
    GridIndirectionScheme(MPI_Comm comm) : comm_(comm) {
        MPI_Comm_rank(comm_, &my_rank_);
        MPI_Comm_size(comm_, &my_size_);
        grid_size_ = static_cast<int>(std::round(std::sqrt(my_size_)));
    }

    [[nodiscard]] PEID next_hop(PEID /*sender*/, PEID receiver) const {
        auto proxy = get_proxy(rank(), receiver);
        return proxy;
    }

    [[nodiscard]] bool should_redirect(PEID /*sender*/, PEID receiver) const {
        return receiver != rank();
    }

    [[nodiscard]] auto num_groups() const -> std::size_t {
        return grid_size_;
    }

    [[nodiscard]] auto group_size() const -> std::size_t {
        return grid_size_;
    }

private:
    [[nodiscard]] int rank() const {
        return my_rank_;
    }

    [[nodiscard]] int size() const {
        return my_size_;
    }

    struct GridPosition {
        int row;
        int column;
        bool operator==(const GridPosition& rhs) const {
            return row == rhs.row && column == rhs.column;
        }
    };

    [[nodiscard]] GridPosition rank_to_grid_position(PEID mpi_rank) const {
        return GridPosition{.row = mpi_rank / grid_size_, .column = mpi_rank % grid_size_};
    }

    [[nodiscard]] PEID grid_position_to_rank(GridPosition grid_position) const {
        return (grid_position.row * grid_size_) + grid_position.column;
    }

    [[nodiscard]] PEID get_proxy(PEID from, PEID to) const {  // NOLINT(readability-identifier-length)
        KASSERT(from < my_size_);
	KASSERT(to < my_size_);
        auto from_pos = rank_to_grid_position(from);
        auto to_pos = rank_to_grid_position(to);
        GridPosition proxy = {.row = from_pos.row, .column = to_pos.column};
        if (grid_position_to_rank(proxy) >= size()) {
            proxy = {.row = from_pos.column, .column = to_pos.column};
        }
        if (proxy == from_pos) {
            proxy = to_pos;
        }
        KASSERT(grid_position_to_rank(proxy) < size());
        return grid_position_to_rank(proxy);
    }
    MPI_Comm comm_;
    PEID grid_size_ = 0;
    int my_rank_ = 0;
    int my_size_ = 0;
};

}  // namespace briefkasten
