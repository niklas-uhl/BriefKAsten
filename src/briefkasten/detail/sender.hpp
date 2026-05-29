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

#include <deque>
#include <kamping/mpi_datatype.hpp>
#include <limits>
#include <optional>
#include <ranges>

#include <mpi.h>

#include "./concepts.hpp"
#include "./request_pool.hpp"

namespace briefkasten {
template <MPIBuffer MessageContainer>
class Sender {
public:
    using value_type = std::ranges::range_value_t<MessageContainer>;
    // NOLINTBEGIN(*-easily-swappable-parameters)
    Sender(MPI_Comm comm,
           std::size_t num_send_slots,
           std::size_t send_backlog_capacity)  // NOLINTEND(*-easily-swappable-parameters)
        : comm_(comm),
          request_pool_(num_send_slots),
          active_sends_(num_send_slots),
          send_backlog_capacity_(send_backlog_capacity) {}

    std::optional<std::size_t> enqueue_for_sending(MessageContainer&& message, PEID destination, int tag) {
        drain_send_backlog();  // try to send as many as possible

        std::size_t receipt = next_receipt_id_;
        PendingSend msg{.send = ActiveSend{.receipt = receipt, .message = std::move(message)},
                        .destination = destination,
                        .tag = tag};

        if (request_pool_.inactive_requests() > 0 && send_backlog_.empty()) {
            // we can send immediatly
            auto request = request_pool_.get_some_inactive_request();
            KASSERT(request.has_value(), "There should be inactive requests.");
            start_send(std::move(msg), request->second, request->first);
        } else if (send_backlog_.size() < send_backlog_capacity_) {
            // buffer the message
            send_backlog_.emplace_back(std::move(msg));
        } else {
            // no room for buffering or sending
            return std::nullopt;
        }
        next_receipt_id_++;
        return receipt;
    };

    auto progress_sending(SendFinishedCallback<MessageContainer> auto&& on_finished_sending) {
        constexpr bool move_back_buffer = std::invocable<decltype(on_finished_sending), std::size_t, MessageContainer>;
        // check for finished sends and try starting new ones
        bool any_completed = request_pool_.test_any([&](int completed_request_index) {
            std::optional<ActiveSend>& completed_send = active_sends_[completed_request_index];
            KASSERT(completed_send.has_value());
            std::size_t receipt = completed_send->receipt;
            MessageContainer buffer = std::move(completed_send->message);
            completed_send.reset();
            if constexpr (move_back_buffer) {
                on_finished_sending(receipt, std::move(buffer));
            } else {
                on_finished_sending(receipt);
            }
            if (!send_backlog_.empty()) {
                auto request = request_pool_.get_some_inactive_request(completed_request_index);
                KASSERT(request.has_value(), "We just completed a send, so the slot we hinted should be free.");
                start_send(std::move(send_backlog_.front()), request->second, request->first);
                send_backlog_.pop_front();
            }
        });
        // fill the remaining slots if possible
        drain_send_backlog();
        return any_completed;
    };

    [[nodiscard]] bool has_capacity() const {
        if (send_backlog_capacity_ == std::numeric_limits<std::size_t>::max()) {
            return true;
        }
        return send_backlog_.size() < send_backlog_capacity_ || request_pool_.inactive_requests() > 0;
    }

    [[nodiscard]] std::size_t outstanding_sends() const {
        return send_backlog_.size() + request_pool_.active_requests();
    }

private:
    struct ActiveSend {
        std::size_t receipt;
        MessageContainer message;
    };
    struct PendingSend {
        ActiveSend send;
        PEID destination;
        int tag;
    };

    void start_send(PendingSend&& msg,  // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
                    MPI_Request& request,
                    std::size_t request_index) {
        auto& active_send = active_sends_[request_index];
        active_send = std::move(msg.send);
#if MPI_VERSION >= 4
        MPI_Isend_c(active_send->message.data(), active_send->message.size(),
                    kamping::mpi_datatype<value_type>(), msg.destination, msg.tag, comm_, &request);
#else
        MPI_Isend(active_send->message.data(), static_cast<int>(active_send->message.size()),
                  kamping::mpi_datatype<value_type>(), msg.destination, msg.tag, comm_, &request);
#endif
    }

    void drain_send_backlog() {
        while (!send_backlog_.empty() && request_pool_.inactive_requests() > 0) {
            auto request = request_pool_.get_some_inactive_request();
            KASSERT(request.has_value(), "There should be some inactive request.");
            start_send(std::move(send_backlog_.front()), request->second, request->first);
            send_backlog_.pop_front();
        }
    }

    MPI_Comm comm_;
    internal::RequestPool request_pool_;
    std::vector<std::optional<ActiveSend>> active_sends_;
    std::deque<PendingSend> send_backlog_;
    std::size_t send_backlog_capacity_;
    int next_receipt_id_ = 0;
};
}  // namespace briefkasten
