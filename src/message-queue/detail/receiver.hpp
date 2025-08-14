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

#include <cstddef>
#include <kamping/environment.hpp>
#include <ranges>
#include <span>
#include <vector>

#include <mpi.h>
#include <kamping/mpi_datatype.hpp>

#include "./concepts.hpp"
#include "./termination_counter.hpp"

namespace message_queue {
namespace internal {
auto build_envelope(MPIBuffer auto const& buffer, MPI_Status& status, int rank)
    -> MessageEnvelope<std::span<const std::ranges::range_value_t<decltype(buffer)>>> {
    using T = std::ranges::range_value_t<decltype(buffer)>;
    MPI_Count count = 0;
    MPI_Get_count_c(&status, kamping::mpi_datatype<T>(), &count);
    KASSERT(count <= buffer.size());
    std::span<const T> message = std::span(buffer).first(count);
    auto envelope = MessageEnvelope<std::span<const T>>{std::move(message), status.MPI_SOURCE, rank, status.MPI_TAG};
    return envelope;
}
}  // namespace internal

template <MPIBuffer ReceiveBufferContainer>
class PersistentReceiver {
public:
    using value_type = std::ranges::range_value_t<ReceiveBufferContainer>;
    // NOLINTBEGIN(*-easily-swappable-parameters)
    PersistentReceiver(MPI_Comm comm,
                       int tag,
                       internal::TerminationCounter& termination_counter,
                       std::size_t num_receive_slots,
                       std::size_t reserved_receive_buffer_size)  // NOLINTEND(*-easily-swappable-parameters)
        : comm_(comm),
          tag_(tag),
          receive_requests_(num_receive_slots, MPI_REQUEST_NULL),
          receive_buffers_(num_receive_slots),
          statuses_(num_receive_slots),
          indices_(num_receive_slots),
          termination_(&termination_counter) {
        KASSERT(tag_ < kamping::mpi_env.tag_upper_bound());
        MPI_Comm_rank(comm, &rank_);
        for (auto [request, buffer] : std::views::zip(receive_requests_, receive_buffers_)) {
            buffer.resize(reserved_receive_buffer_size);
            MPI_Recv_init_c(buffer.data(),                        // buf
                            buffer.size(),                        // count
                            kamping::mpi_datatype<value_type>(),  // datatype
                            MPI_ANY_SOURCE,                       // source
                            tag_,                                 // tag
                            comm_,                                // comm
                            &request                              // request
            );
            MPI_Start(&request);
        }
    }

    ~PersistentReceiver() {
        std::vector<MPI_Status> statuses(receive_requests_.size());
        for (MPI_Request& request : receive_requests_) {
            MPI_Cancel(&request);
        }
        MPI_Waitall(static_cast<int>(receive_requests_.size()), receive_requests_.data(), statuses.data());
        for (auto [request, status] : std::views::zip(receive_requests_, statuses)) {
            int cancelled = 0;
            MPI_Test_cancelled(&status, &cancelled);
            KASSERT(cancelled,
                    "Receiver's destructor will only be called when communication is finished, so all persistent "
                    "requests should gracefully cancel.");
            MPI_Request_free(&request);
        }
    }

    PersistentReceiver(const PersistentReceiver&) = delete;

    PersistentReceiver(PersistentReceiver&& other) noexcept
        : comm_(other.comm_),
          tag_(other.tag_),
          receive_requests_(std::move(other.receive_requests_)),
          receive_buffers_(std::move(other.receive_buffers_)),
	  statuses_(std::move(other.statuses_)),
	  indices_(std::move(other.indices_)),
          termination_(other.termination_),
          rank_(other.rank_) {
        other.termination_ = nullptr;
    }

    PersistentReceiver& operator=(const PersistentReceiver&) = delete;

    PersistentReceiver& operator=(PersistentReceiver&& other) noexcept {
        comm_ = other.comm_;
        tag_ = other.tag_;
        receive_requests_ = std::move(other.receive_requests_);
        receive_buffers_ = std::move(other.receive_buffers_);
        statuses_ = std::move(other.statuses_);
	indices_ = std::move(other.indices_);
        termination_ = other.termination_;
        rank_ = other.rank_;
        other.termination_ = nullptr;
    }

    bool probe_for_one_message(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        int& index = indices_[0];
        int request_completed = 0;
        MPI_Status& status = statuses_[0];
        MPI_Testany(static_cast<int>(receive_requests_.size()),  // count
                    receive_requests_.data(),                    // array_of_requests
                    &index,                                      // indx
                    &request_completed,                          // flag
                    &status);                                    // status
        if (!request_completed) {
            return false;
        }
        KASSERT(index != MPI_UNDEFINED, "This should not happen, because we always have pending receive requests.");
        termination_->track_receive();
        ReceiveBufferContainer& buffer = receive_buffers_[index];
        auto envelope = build_envelope(buffer, status, rank_);
        on_message(std::move(envelope));
        MPI_Start(receive_requests_[index]);
        return true;
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        int num_completed = 0;
        MPI_Status* status = statuses_.data();
	assert(status != nullptr);
        MPI_Testsome(static_cast<int>(receive_requests_.size()),  // count
                     receive_requests_.data(),                    // array_of_requests
                     &num_completed,                              // outcount
                     indices_.data(),                             // indices
                     status);                                     // array_of_statuses
        if (num_completed == 0) {
            return false;
        }
        KASSERT(num_completed != MPI_UNDEFINED,
                "This should not happen, because we always have pending receive requests.");
        auto indices = std::span(indices_).first(num_completed);
        auto statuses = std::span(statuses_).first(num_completed);
        auto buffers = indices | std::views::transform([&](int index) -> auto& { return receive_buffers_[index]; });
        auto requests = indices | std::views::transform([&](int index) -> auto& { return receive_requests_[index]; });

        for (auto [buffer, status, request] : std::views::zip(buffers, statuses, requests)) {
            termination_->track_receive();
            auto envelope = internal::build_envelope(buffer, status, rank_);
            on_message(std::move(envelope));
            MPI_Start(&request);
        }
        return true;
    }

    void resize_buffers(std::size_t new_size, MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        std::vector<MPI_Status> statuses(receive_requests_.size());
        for (MPI_Request& request : receive_requests_) {
            MPI_Cancel(&request);
        }
        MPI_Waitall(static_cast<int>(receive_requests_.size()), receive_requests_.data(), statuses.data());
        for (auto& [buffer, request, status] : std::views::zip(receive_buffers_, receive_requests_, statuses)) {
            int cancelled = 0;
            MPI_Test_cancelled(&status, &cancelled);
            if (!cancelled) {
                termination_->track_receive();
                auto envelope = build_envelope(buffer, status);
                on_message(std::move(envelope));
            }
            MPI_Request_free(&request);
            buffer.resize(new_size);
            MPI_Recv_init_c(buffer.data(),                        // buf
                            buffer.size(),                        // count
                            kamping::mpi_datatype<value_type>(),  // datatype
                            MPI_ANY_SOURCE,                       // source
                            tag_,                                 // tag
                            comm_,                                // comm
                            &request                              // request
            );
        }
    }

    [[nodiscard]] std::size_t buffer_size() const {
        return receive_buffers_.front().size();
    }

private:
    MPI_Comm comm_;
    int tag_;
    std::vector<MPI_Request> receive_requests_;
    std::vector<ReceiveBufferContainer> receive_buffers_;
    std::vector<MPI_Status> statuses_;
    std::vector<int> indices_;
    internal::TerminationCounter* termination_;
    int rank_ = 0;
};

template <MPIBuffer ReceiveBufferContainer>
class ProbeReceiver {
public:
    using value_type = std::ranges::range_value_t<ReceiveBufferContainer>;
    // NOLINTBEGIN(*-easily-swappable-parameters)
    ProbeReceiver(MPI_Comm comm,
                  int tag,
                  internal::TerminationCounter& termination_counter,
                  std::size_t num_receive_slots,
                  std::size_t reserved_receive_buffer_size)  // NOLINTEND(*-easily-swappable-parameters)
        : comm_(comm),
          tag_(tag),
          receive_buffers_(num_receive_slots),
          receive_requests_(num_receive_slots, MPI_REQUEST_NULL),
          termination_(&termination_counter),
          statuses_(num_receive_slots) {
        KASSERT(tag < kamping::mpi_env.tag_upper_bound());
        MPI_Comm_rank(comm_, &rank_);
        for (ReceiveBufferContainer& buffer : receive_buffers_) {
            buffer.resize(reserved_receive_buffer_size);
        }
    }
    bool probe_for_one_message(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        MPI_Message message = MPI_MESSAGE_NULL;
        MPI_Status status;
        int probe_successful = 0;
        MPI_Improbe(MPI_ANY_SOURCE, tag_, comm_, &probe_successful, &message, &status);
        if (!probe_successful) {
            return false;
        }
        auto& buffer = receive_buffers_.front();
        MPI_Mrecv_c(buffer.data(), buffer.size(), kamping::mpi_datatype<value_type>(), &message, &status);
        termination_->track_receive();
        auto envelope = internal::build_envelope(buffer, status, rank_);
        on_message(std::move(envelope));
        return true;
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        probe_for_messages(std::forward<decltype(on_message)>(on_message), receive_buffers_.size());
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message,
                            std::size_t max_receives) {
        MPI_Message message = MPI_MESSAGE_NULL;
        MPI_Status status;
        int probe_successful = 1;
        std::size_t round = 0;
        std::size_t num_recvs = 0;
        auto receive_all = [&]() {
            if (num_recvs == 0) {
                return;
            }
            MPI_Waitall(num_recvs, receive_requests_.data(), statuses_.data());
            auto buffers = std::span(receive_buffers_).first(num_recvs);
            for (auto& [buffer, status] : std::views::zip(buffers, statuses_)) {
                termination_->track_receive();
                auto envelope = internal::build_envelope(buffer, status, rank_);
                on_message(std::move(envelope));
            }

            num_recvs = 0;
        };
        while (probe_successful && round < max_receives) {
            MPI_Improbe(MPI_ANY_SOURCE, tag_, comm_, &probe_successful, &message, &status);
            if (!probe_successful) {
                continue;
            }
            auto& buffer = receive_buffers_[num_recvs];
            auto& request = receive_requests_[num_recvs];

            MPI_Imrecv_c(buffer.data(), buffer.size(), kamping::mpi_datatype<value_type>(), &message, &request);
            num_recvs++;
            if (num_recvs == receive_buffers_.size()) {
                receive_all();
            }
            round++;
        }
        receive_all();
        return round == 0;  // No messages available
    }

    void resize_buffers(std::size_t new_size, MessageHandler<value_type, std::span<value_type>> auto&& /*on_message*/) {
        for (auto& buffer : receive_buffers_) {
            buffer.resize(new_size);
        }
    }

private:
    MPI_Comm comm_;
    int tag_;
    std::vector<ReceiveBufferContainer> receive_buffers_;
    std::vector<MPI_Request> receive_requests_;
    std::vector<MPI_Status> statuses_;
    internal::TerminationCounter* termination_;
    int rank_ = 0;
};

template <MPIBuffer ReceiveBufferContainer>
class AllocatingProbeReceiver {
public:
    using value_type = std::ranges::range_value_t<ReceiveBufferContainer>;
    // NOLINTBEGIN(*-easily-swappable-parameters)
    AllocatingProbeReceiver(
        MPI_Comm comm,
        int tag,
        internal::TerminationCounter& termination_counter)  // NOLINTEND(*-easily-swappable-parameters)
        : comm_(comm), tag_(tag), receive_buffers_(), termination_(&termination_counter) {
        KASSERT(tag < kamping::mpi_env.tag_upper_bound());
        MPI_Comm_rank(comm_, &rank_);
    }
    bool probe_for_one_message(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        MPI_Message message = MPI_MESSAGE_NULL;
        MPI_Status status;
        int probe_successful = 0;
        MPI_Improbe(MPI_ANY_SOURCE, tag_, comm_, &probe_successful, &message, &status);
        if (!probe_successful) {
            return false;
        }

        ReceiveBufferContainer buffer;
        MPI_Count count = 0;
        MPI_Get_count_c(&status, kamping::mpi_datatype<value_type>(), &count);
        buffer.resize(count);
        MPI_Mrecv_c(buffer.data(), buffer.size(), kamping::mpi_datatype<value_type>(), &message, &status);
        termination_->track_receive();
        auto envelope =
            MessageEnvelope<ReceiveBufferContainer>{std::move(buffer), status.MPI_SOURCE, rank_, status.MPI_TAG};
        on_message(std::move(envelope));
        return true;
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message,
                            std::size_t max_receives) {
        MPI_Message message = MPI_MESSAGE_NULL;
        MPI_Status status;
        int probe_successful = 1;
        std::size_t round = 0;
        while (probe_successful && round < max_receives) {
            MPI_Improbe(MPI_ANY_SOURCE, tag_, comm_, &probe_successful, &message, &status);
            if (!probe_successful) {
                continue;
            }
            MPI_Count count = 0;
            MPI_Get_count_c(&status, kamping::mpi_datatype<value_type>(), &count);
            auto& buffer = receive_buffers_.emplace_back(count);
            auto& request = receive_requests_.emplace_back(MPI_REQUEST_NULL);

            MPI_Imrecv_c(buffer.data(), buffer.size(), kamping::mpi_datatype<value_type>(), &message, &request);
            round++;
        }
        if (round == 0) {
            return false;
        }
        statuses_.resize(receive_requests_.size());
        MPI_Waitall(static_cast<int>(receive_requests_.size()), receive_requests_.data(), statuses_.data());
        for (auto [buffer, status] : std::views::zip(receive_buffers_, statuses_)) {
            termination_->track_receive();
            auto envelope =
                MessageEnvelope<ReceiveBufferContainer>{std::move(buffer), status.MPI_SOURCE, rank_, status.MPI_TAG};
            on_message(std::move(envelope));
        }
        receive_buffers_.resize(0);
        receive_requests_.resize(0);
        statuses_.resize(0);
        return true;
    }

private:
    MPI_Comm comm_;
    int tag_;
    std::vector<ReceiveBufferContainer> receive_buffers_;
    std::vector<MPI_Request> receive_requests_;
    std::vector<MPI_Status> statuses_;
    internal::TerminationCounter* termination_;
    int rank_ = 0;
};

}  // namespace message_queue
