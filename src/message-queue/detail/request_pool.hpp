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
#include <algorithm>
#include <concepts>
#include <cstddef>
#include <kassert/kassert.hpp>
#include <optional>
#include <utility>
#include <vector>

namespace message_queue::internal {

enum class SearchStrategy : std::uint8_t { linear, local };
enum class CompletionStrategy : std::uint8_t { active, all, round_robin };

class RequestPool {
public:
    RequestPool(std::size_t capacity = 0) : requests(capacity, MPI_REQUEST_NULL), indices(capacity) {}

    std::optional<std::pair<int, MPI_Request&>> get_some_inactive_request(
        int hint = -1,
        SearchStrategy search_strategy = SearchStrategy::linear) {
        if (inactive_requests() == 0) {
            return {};
        }

        if (hint < 0) {
            hint = last_slot.value_or(-1);
            last_slot.reset();
        }

        // first check the hinted position
        if (hint >= 0 && static_cast<std::size_t>(hint) < capacity() && requests[hint] == MPI_REQUEST_NULL) {
            add_to_active_range(hint);
            return {{hint, requests[hint]}};
        }
        if (search_strategy == SearchStrategy::linear) {
            auto it = std::ranges::find(requests, MPI_REQUEST_NULL);
            if (it == requests.end()) {
                return std::nullopt;
            }
            std::size_t index = std::distance(requests.begin(), it);
            add_to_active_range(static_cast<int>(index));
            return {{index, *it}};
        }
        KASSERT(search_strategy == SearchStrategy::local);

        // first try to find a request in the active range if there is one
        if (active_requests_ < static_cast<std::size_t>(active_range.second - active_range.first)) {
            for (auto i = active_range.first; i < active_range.second; i++) {
                if (requests[i] == MPI_REQUEST_NULL) {
                    add_to_active_range(i);
                    return {{i, requests[i]}};
                }
            }
        }
        // search right of the active range
        for (auto i = active_range.second; i < static_cast<int>(capacity()); i++) {
            if (requests[i] == MPI_REQUEST_NULL) {
                add_to_active_range(i);
                return {{i, requests[i]}};
            }
        }
        // search left of the active range starting at active_range.first
        for (auto i = active_range.first - 1; i >= 0; i--) {
            if (requests[i] == MPI_REQUEST_NULL) {
                add_to_active_range(i);
                return {{i, requests[i]}};
            }
        }
        // this should never happen
        return {};
    }

    void test_some(
        std::invocable<int> auto&& on_complete = [](int) {},
        CompletionStrategy completion_strategy = CompletionStrategy::all) {
        int outcount = 0;
        auto [incount, request_ptr, index_offset] = [&]() -> std::tuple<int, MPI_Request*, int> {
            switch (completion_strategy) {
                case CompletionStrategy::active:
                    return std::tuple{active_range.second - active_range.first, &requests[active_range.first],
                                      active_range.first};
                default:
                case CompletionStrategy::all:
                    return std::tuple{capacity(), requests.data(), 0};
                case CompletionStrategy::round_robin:
                    return std::tuple{capacity() - round_robin_index, &requests[round_robin_index], round_robin_index};
            }
        }();
        MPI_Testsome(incount,             // count
                     request_ptr,         // requests
                     &outcount,           // outcount
                     indices.data(),      // indices
                     MPI_STATUSES_IGNORE  // statuses
        );
        if (outcount != MPI_UNDEFINED) {
            std::for_each_n(indices.begin(), outcount, [&](int index) {
                // map index to the global index
                index += index_offset;
                remove_from_active_range(index);
                on_complete(index);
            });
        }
        if (completion_strategy == CompletionStrategy::round_robin) {
            // we check the remaining requests before the round_robin_index
            MPI_Testsome(round_robin_index,   // count
                         requests.data(),     // requests
                         &outcount,           // outcount
                         indices.data(),      // indices
                         MPI_STATUSES_IGNORE  // statuses
            );
            if (outcount != MPI_UNDEFINED) {
                std::for_each_n(indices.begin(), outcount, [&](int index) {
                    remove_from_active_range(index);
                    on_complete(index);
                });
            }
            round_robin_index++;
            if (round_robin_index == capacity()) {
                round_robin_index = 0;
            }
        }
    }

    bool test_any(
        std::invocable<int> auto&& on_complete = [](int) {},
        CompletionStrategy completion_strategy = CompletionStrategy::all) {
        int flag = 0;
        int index = 0;
        auto [incount, request_ptr, index_offset] = [&]() -> std::tuple<int, MPI_Request*, int> {
            switch (completion_strategy) {
                case CompletionStrategy::active:
                    return std::tuple{active_range.second - active_range.first, &requests[active_range.first],
                                      active_range.first};
                default:
                case CompletionStrategy::all:
                    return std::tuple{capacity(), requests.data(), 0};
                case CompletionStrategy::round_robin:
                    return std::tuple{capacity() - round_robin_index, &requests[round_robin_index], round_robin_index};
            }
        }();
        MPI_Testany(incount,           // count
                    request_ptr,       // requests
                    &index,            // index
                    &flag,             // flag
                    MPI_STATUS_IGNORE  // status
        );
        if (flag && index != MPI_UNDEFINED) {
            index += index_offset;
            remove_from_active_range(index);
            on_complete(index);
        } else if (completion_strategy == CompletionStrategy::round_robin) {
            MPI_Testany(round_robin_index, requests.data(), &index, &flag, MPI_STATUS_IGNORE);
            if (flag && index != MPI_UNDEFINED) {
                remove_from_active_range(index);
                on_complete(index);
            }
            round_robin_index++;
            if (round_robin_index == static_cast<int>(capacity())) {
                round_robin_index = 0;
            }
        }
        return static_cast<bool>(flag);
    }

    [[nodiscard]] std::size_t active_requests() const {
        return active_requests_;
    }

    [[nodiscard]] std::size_t inactive_requests() const {
        return capacity() - active_requests();
    }

    [[nodiscard]] std::size_t capacity() const {
        return requests.size();
    }

private:
    void add_to_active_range(int index) {
        active_requests_++;
        active_range.first = std::min(index, active_range.first);
        active_range.second = std::max(index + 1, active_range.second);
    }

    void remove_from_active_range(int index) {
        last_slot = index;
        active_requests_--;
        if (index == active_range.first) {
            active_range.first++;
        } else {
            if (index + 1 == active_range.second) {
                active_range.second--;
            }
        }
    }
    std::vector<MPI_Request> requests;
    std::vector<int> indices;
    std::optional<int> last_slot;
    size_t inactive_request_pointer = 0;
    size_t active_requests_ = 0;
    std::pair<int, int> active_range = {0, 0};
    int round_robin_index = 0;
};
}  // namespace message_queue::internal
