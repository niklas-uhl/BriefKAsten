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
#include "./detail/link_class.hpp"

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

    /// Number of column-groups, i.e. the first-hop fan-out: a rank sends to one proxy per column within its row.
    [[nodiscard]] auto num_groups() const -> std::size_t {
        return grid_size_;
    }

    /// Which obligation a link to \p peer puts on its far end; see LinkClass.
    ///
    /// Same column means terminal: get_proxy reaches a destination in our own column in one hop (the
    /// natural proxy {our row, their column} is then us, and the `proxy == from_pos` branch redirects
    /// straight to the destination), so a same-column link never carries first-hop traffic. Everything
    /// else -- our own row, and the ragged fallback {our column, their column} -- may have to be relayed.
    /// Symmetric, as LinkClass requires: "same column" is the same relation read from either end.
    [[nodiscard]] LinkClass link_class(PEID peer) const {
        if (peer == rank()) {
            return LinkClass::to_destination;  // not a link; never classified in anger
        }
        return rank_to_grid_position(peer).column == rank_to_grid_position(rank()).column
                   ? LinkClass::to_destination
                   : LinkClass::to_proxy;
    }

    /// Size of a column-group (number of rows), i.e. the second-hop fan-out: a proxy forwards within a column to every
    /// final receiver in it. For a non-square grid this exceeds num_groups by up to one, and it is >= both hops'
    /// fan-out, so callers can use it as a single conservative bound for sizing both hop queues.
    [[nodiscard]] auto group_size() const -> std::size_t {
        return (static_cast<std::size_t>(my_size_) + grid_size_ - 1) / grid_size_;
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
