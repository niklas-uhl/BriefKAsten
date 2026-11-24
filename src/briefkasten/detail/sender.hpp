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

#include <algorithm>
#include <deque>
#include <kamping/mpi_datatype.hpp>
#include <limits>
#include <optional>
#include <ranges>

#include <mpi.h>

#include "./concepts.hpp"
#include "./request_pool.hpp"
#ifdef BRIEFKASTEN_CHECK_BUFFERS
#include "./buffer_check.hpp"
#endif

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
          send_backlog_capacity_(send_backlog_capacity) {
#ifdef BRIEFKASTEN_CHECK_BUFFERS
        send_records_.resize(num_send_slots);
#endif
    }

#ifdef BRIEFKASTEN_CHECK_BUFFERS
    // A Sender destroyed or overwritten with a send in flight frees the send's buffer under MPI.
    Sender(Sender const&) = delete;
    Sender& operator=(Sender const&) = delete;
    Sender(Sender&&) noexcept = default;  // leaves `other` without sends (moved-from vectors are empty)
    Sender& operator=(Sender&& other) noexcept {
        check_no_sends_in_flight("Sender overwritten (move assignment)");
        comm_ = other.comm_;
        request_pool_ = std::move(other.request_pool_);
        active_sends_ = std::move(other.active_sends_);
        send_records_ = std::move(other.send_records_);
        send_backlog_ = std::move(other.send_backlog_);
        send_backlog_capacity_ = other.send_backlog_capacity_;
        next_receipt_id_ = other.next_receipt_id_;
        num_immediate_sends_ = other.num_immediate_sends_;
        num_backlogged_sends_ = other.num_backlogged_sends_;
        num_send_capacity_misses_ = other.num_send_capacity_misses_;
        peak_send_backlog_ = other.peak_send_backlog_;
        return *this;
    }
    ~Sender() {
        check_no_sends_in_flight("Sender destroyed");
    }
#endif

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
            num_immediate_sends_++;
        } else if (send_backlog_.size() < send_backlog_capacity_) {
            // buffer the message
            send_backlog_.emplace_back(std::move(msg));
            num_backlogged_sends_++;
            peak_send_backlog_ = std::max(peak_send_backlog_, send_backlog_.size());
        } else {
            // no room for buffering or sending
            num_send_capacity_misses_++;
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
#ifdef BRIEFKASTEN_CHECK_BUFFERS
            check_completed_send(completed_request_index);
#endif
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

    /// Adjust the send backlog capacity at runtime. Only ever grown by the indirection adapter; shrinking below the
    /// current backlog size is harmless (no further messages are buffered until it drains below the new cap).
    void set_send_backlog_capacity(std::size_t send_backlog_capacity) {
        send_backlog_capacity_ = send_backlog_capacity;
    }

    [[nodiscard]] bool has_capacity() const {
        if (send_backlog_capacity_ == std::numeric_limits<std::size_t>::max()) {
            return true;
        }
        return send_backlog_.size() < send_backlog_capacity_ || request_pool_.inactive_requests() > 0;
    }

    [[nodiscard]] std::size_t outstanding_sends() const {
        return send_backlog_.size() + request_pool_.active_requests();
    }

    /// Sends that found a free request slot and went straight to MPI.
    [[nodiscard]] std::size_t num_immediate_sends() const {
        return num_immediate_sends_;
    }

    /// Sends that had to queue behind a busy request pool. A high ratio against \ref num_immediate_sends means
    /// `num_request_slots` (default 8) is the binding constraint, not the aggregation buffers.
    [[nodiscard]] std::size_t num_backlogged_sends() const {
        return num_backlogged_sends_;
    }

    /// Sends rejected outright because neither a request slot nor backlog room was available. The caller then
    /// spins in a poll loop (see \c BufferedMessageQueue::flush_all_buffers_blocking), which until now was
    /// invisible: \c num_buffer_stalls only covers aggregation-buffer exhaustion.
    [[nodiscard]] std::size_t num_send_capacity_misses() const {
        return num_send_capacity_misses_;
    }

    /// High-water mark of the backlog deque, to compare against `send_backlog_capacity`.
    [[nodiscard]] std::size_t peak_send_backlog() const {
        return peak_send_backlog_;
    }

    void reset_counters() {
        num_immediate_sends_ = 0;
        num_backlogged_sends_ = 0;
        num_send_capacity_misses_ = 0;
        peak_send_backlog_ = 0;
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
#ifdef BRIEFKASTEN_CHECK_BUFFERS
        if (active_send.has_value() || request != MPI_REQUEST_NULL) {
            internal::buffer_check_failed("send slot reused while its send is in flight: its buffer is freed under MPI");
        }
#endif
        active_send = std::move(msg.send);
#if MPI_VERSION >= 4
        MPI_Isend_c(active_send->message.data(), active_send->message.size(),
                    kamping::mpi_datatype<value_type>(), msg.destination, msg.tag, comm_, &request);
#else
        MPI_Isend(active_send->message.data(), static_cast<int>(active_send->message.size()),
                  kamping::mpi_datatype<value_type>(), msg.destination, msg.tag, comm_, &request);
#endif
#ifdef BRIEFKASTEN_CHECK_BUFFERS
        send_records_[request_index] = internal::BufferRecord{.data = active_send->message.data(),
                                                              .size = active_send->message.size(),
                                                              .checksum = internal::buffer_checksum(active_send->message)};
#endif
    }

#ifdef BRIEFKASTEN_CHECK_BUFFERS
    /// The send of slot \p index completed: MPI must have released the request, and the buffer must be the one given to
    /// MPI_Isend, unchanged.
    void check_completed_send(int index) {
        auto const slot = static_cast<std::size_t>(index);
        if (request_pool_.is_active(index)) {
            internal::buffer_check_failed("send reported complete while MPI still holds its request");
        }
        auto const& message = active_sends_[slot]->message;
        auto const& record = send_records_[slot];
        if (static_cast<void const*>(message.data()) != record.data || message.size() != record.size) {
            internal::buffer_check_failed("send buffer moved or resized while in flight");
        }
        if (internal::buffer_checksum(message) != record.checksum) {
            internal::buffer_check_failed("send buffer modified while in flight");
        }
        send_records_[slot] = {};
    }

    void check_no_sends_in_flight(char const* what) const {
        for (std::size_t slot = 0; slot < active_sends_.size(); ++slot) {
            if (active_sends_[slot].has_value() && request_pool_.is_active(static_cast<int>(slot))) {
                internal::buffer_check_failed(what);
            }
        }
    }
#endif

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
#ifdef BRIEFKASTEN_CHECK_BUFFERS
    std::vector<internal::BufferRecord> send_records_;  ///< per slot, what was given to MPI_Isend
#endif
    std::deque<PendingSend> send_backlog_;
    std::size_t send_backlog_capacity_;
    int next_receipt_id_ = 0;
    std::size_t num_immediate_sends_ = 0;
    std::size_t num_backlogged_sends_ = 0;
    std::size_t num_send_capacity_misses_ = 0;
    std::size_t peak_send_backlog_ = 0;
};
}  // namespace briefkasten
