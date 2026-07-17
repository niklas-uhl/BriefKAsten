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

#ifdef BRIEFKASTEN_CXX20
#include <range/v3/view/zip.hpp>
#endif

namespace briefkasten {
namespace internal {
auto build_envelope(MPIBuffer auto const& buffer, MPI_Status& status, int rank)
    -> MessageEnvelope<std::span<const std::ranges::range_value_t<decltype(buffer)>>> {
    using T = std::ranges::range_value_t<decltype(buffer)>;
#if MPI_VERSION >= 4
    MPI_Count count = 0;
    MPI_Get_count_c(&status, kamping::mpi_datatype<T>(), &count);
#else
    int count = 0;
    MPI_Get_count(&status, kamping::mpi_datatype<T>(), &count);
#endif
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
          statuses_(1, std::vector<MPI_Status>(num_receive_slots)),
          indices_(1, std::vector<int>(num_receive_slots)),
          termination_(&termination_counter) {
        KASSERT(tag_ < kamping::Environment<>::tag_upper_bound());
        MPI_Comm_rank(comm, &rank_);
#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto [request, buffer] : views::zip(receive_requests_, receive_buffers_)) {
            buffer.resize(reserved_receive_buffer_size);
#if MPI_VERSION >= 4
            MPI_Recv_init_c(buffer.data(),                        // buf
                            buffer.size(),                        // count
                            kamping::mpi_datatype<value_type>(),  // datatype
                            MPI_ANY_SOURCE,                       // source
                            tag_,                                 // tag
                            comm_,                                // comm
                            &request                              // request
            );
#else
            MPI_Recv_init(buffer.data(),                        // buf
                          static_cast<int>(buffer.size()),      // count
                          kamping::mpi_datatype<value_type>(),  // datatype
                          MPI_ANY_SOURCE,                       // source
                          tag_,                                 // tag
                          comm_,                                // comm
                          &request                              // request
            );
#endif
            MPI_Start(&request);
        }
    }

    ~PersistentReceiver() {
        std::vector<MPI_Status> statuses(receive_requests_.size());
        for (MPI_Request& request : receive_requests_) {
            MPI_Cancel(&request);
        }
        MPI_Waitall(static_cast<int>(receive_requests_.size()), receive_requests_.data(), statuses.data());
#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto [request, status] : views::zip(receive_requests_, statuses)) {
            int cancelled = 0;
            MPI_Test_cancelled(&status, &cancelled);
            KASSERT(cancelled,
                    "Receiver's destructor will only be called when communication is finished, so all persistent "
                    "requests should gracefully cancel.");
            MPI_Request_free(&request);
        }
    }

    PersistentReceiver(const PersistentReceiver&) = delete;

    PersistentReceiver(PersistentReceiver&& other) = default;

    PersistentReceiver& operator=(const PersistentReceiver&) = delete;

    PersistentReceiver& operator=(PersistentReceiver&& other) = default;

    void rebind_termination_counter(internal::TerminationCounter& termination_counter) {
        termination_ = &termination_counter;
    }

    bool probe_for_one_message(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        auto [statuses_buf, indices_buf] = step_probe_recursion();
        int& index = indices_buf[0];
        int request_completed = 0;
        MPI_Status& status = statuses_buf[0];
        MPI_Testany(static_cast<int>(receive_requests_.size()),  // count
                    receive_requests_.data(),                    // array_of_requests
                    &index,                                      // indx
                    &request_completed,                          // flag
                    &status);                                    // status
        if (!request_completed || index == MPI_UNDEFINED) {
            unstep_probe_recursion();
            return false;
        }
        termination_->track_receive();
        ReceiveBufferContainer& buffer = receive_buffers_[index];
        auto envelope = build_envelope(buffer, status, rank_);
        on_message(std::move(envelope));
        MPI_Start(&receive_requests_[index]);
        unstep_probe_recursion();
        return true;
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        // calling probe for messages recursively (i.e. via indirection), might lead to corruption of indices and
        // statuses buffers. Therefore we track the recursion depth and allocate more buffers if needed.
        // TODO: make this scope guarded
        auto [statuses_buf, indices_buf] = step_probe_recursion();
        int num_completed = 0;
        MPI_Testsome(static_cast<int>(receive_requests_.size()),  // count
                     receive_requests_.data(),                    // array_of_requests
                     &num_completed,                              // outcount
                     indices_buf.data(),                          // indices
                     statuses_buf.data());                        // array_of_statuses
        if (num_completed == 0 || num_completed == MPI_UNDEFINED) {
            // previously, this code assumed, that all requests are always active
            // but when this method is called recursively in the message handler, e.g. when using indirection,
            // it is possible that some requests have finished somewhere up the call stack and have not been restarted
            // yet.
            unstep_probe_recursion();
            return false;
        }
        auto indices = std::span(indices_buf).first(num_completed);
        auto statuses = std::span(statuses_buf).first(num_completed);
        auto buffers = indices | std::views::transform([&](int index) -> auto& { return receive_buffers_[index]; });
        auto requests = indices | std::views::transform([&](int index) -> auto& { return receive_requests_[index]; });

#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto [buffer, status, request] : views::zip(buffers, statuses, requests)) {
            termination_->track_receive();
            auto envelope = internal::build_envelope(buffer, status, rank_);
            on_message(std::move(envelope));
            MPI_Start(&request);
        }
        unstep_probe_recursion();
        return true;
    }

    void resize_buffers(std::size_t new_size, MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        std::vector<MPI_Status> statuses(receive_requests_.size());
        for (MPI_Request& request : receive_requests_) {
            MPI_Cancel(&request);
        }
        MPI_Waitall(static_cast<int>(receive_requests_.size()), receive_requests_.data(), statuses.data());
#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto&& [buffer, request, status] : views::zip(receive_buffers_, receive_requests_, statuses)) {
            int cancelled = 0;
            MPI_Test_cancelled(&status, &cancelled);
            if (!cancelled) {
                termination_->track_receive();
                auto envelope = build_envelope(buffer, status, rank_);
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
    auto step_probe_recursion() -> std::tuple<std::vector<MPI_Status>&, std::vector<int>&> {
        probe_recursion_depth_++;
        if (probe_recursion_depth_ >= static_cast<int>(statuses_.size())) {
            statuses_.emplace_back(receive_requests_.size());
            indices_.emplace_back(receive_requests_.size());
        }
        return {statuses_[probe_recursion_depth_], indices_[probe_recursion_depth_]};
    }

    auto unstep_probe_recursion() -> void {
        probe_recursion_depth_--;
    }

    MPI_Comm comm_;
    int tag_;
    std::vector<MPI_Request> receive_requests_;
    std::vector<ReceiveBufferContainer> receive_buffers_;
    std::vector<std::vector<MPI_Status>> statuses_;
    std::vector<std::vector<int>> indices_;
    int probe_recursion_depth_ = 0;  // FIXME step_probe_recursion increments before use, so statuses_[0]/indices_[0] are never accessed
    internal::TerminationCounter* termination_;
    int rank_ = 0;
};

// Like PersistentReceiver, this keeps num_receive_slots MPI_ANY_SOURCE receives pre-posted so incoming messages are
// absorbed by MPI progress during any MPI call (decoupled from handler execution) — the property that keeps it draining
// while blocked deep in a recursive handler and thereby avoids the ProbeReceiver livelock. The one difference: it
// re-arms with a *fresh* MPI_Irecv after each completion instead of MPI_Start-ing a reused MPI_Recv_init request.
// Persistent (Recv_init + repeated Start) requests are never freed until teardown, and some MPI stacks (observed on
// PSM2/OmniPath) leak internal receive handles across restarts until the process runs out; a non-persistent request is
// created and freed each cycle, so nothing accumulates. Memory and back-pressure are identical to PersistentReceiver
// (bounded at num_receive_slots buffers).
template <MPIBuffer ReceiveBufferContainer>
class PrepostingReceiver {
public:
    using value_type = std::ranges::range_value_t<ReceiveBufferContainer>;
    // NOLINTBEGIN(*-easily-swappable-parameters)
    PrepostingReceiver(MPI_Comm comm,
                       int tag,
                       internal::TerminationCounter& termination_counter,
                       std::size_t num_receive_slots,
                       std::size_t reserved_receive_buffer_size)  // NOLINTEND(*-easily-swappable-parameters)
        : comm_(comm),
          tag_(tag),
          receive_requests_(num_receive_slots, MPI_REQUEST_NULL),
          receive_buffers_(num_receive_slots),
          statuses_(1, std::vector<MPI_Status>(num_receive_slots)),
          indices_(1, std::vector<int>(num_receive_slots)),
          termination_(&termination_counter) {
        KASSERT(tag_ < kamping::Environment<>::tag_upper_bound());
        MPI_Comm_rank(comm, &rank_);
#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto [request, buffer] : views::zip(receive_requests_, receive_buffers_)) {
            buffer.resize(reserved_receive_buffer_size);
            post_receive(buffer, request);
        }
    }

    ~PrepostingReceiver() {
        std::vector<MPI_Status> statuses(receive_requests_.size());
        for (MPI_Request& request : receive_requests_) {
            if (request != MPI_REQUEST_NULL) {
                MPI_Cancel(&request);
            }
        }
        // MPI_Wait completes (and deallocates) each non-persistent request, setting it to MPI_REQUEST_NULL. Unlike
        // PersistentReceiver there is no MPI_Request_free to call afterwards — that is exactly the handle that persistent
        // requests keep alive and that some stacks leak across restarts.
        MPI_Waitall(static_cast<int>(receive_requests_.size()), receive_requests_.data(), statuses.data());
    }

    PrepostingReceiver(const PrepostingReceiver&) = delete;
    PrepostingReceiver(PrepostingReceiver&& other) = default;
    PrepostingReceiver& operator=(const PrepostingReceiver&) = delete;
    PrepostingReceiver& operator=(PrepostingReceiver&& other) = default;

    void rebind_termination_counter(internal::TerminationCounter& termination_counter) {
        termination_ = &termination_counter;
    }

    bool probe_for_one_message(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        auto [statuses_buf, indices_buf] = step_probe_recursion();
        int& index = indices_buf[0];
        int request_completed = 0;
        MPI_Status& status = statuses_buf[0];
        MPI_Testany(static_cast<int>(receive_requests_.size()),  // count
                    receive_requests_.data(),                    // array_of_requests
                    &index,                                      // indx
                    &request_completed,                          // flag
                    &status);                                    // status
        if (!request_completed || index == MPI_UNDEFINED) {
            unstep_probe_recursion();
            return false;
        }
        termination_->track_receive();
        ReceiveBufferContainer& buffer = receive_buffers_[index];
        auto envelope = build_envelope(buffer, status, rank_);
        on_message(std::move(envelope));
        post_receive(receive_buffers_[index], receive_requests_[index]);
        unstep_probe_recursion();
        return true;
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        // Recursive calls (via indirection) may find some requests already completed but not yet re-posted further up
        // the stack, so statuses/indices buffers are tracked per recursion depth. Mirrors PersistentReceiver.
        auto [statuses_buf, indices_buf] = step_probe_recursion();
        int num_completed = 0;
        MPI_Testsome(static_cast<int>(receive_requests_.size()),  // count
                     receive_requests_.data(),                    // array_of_requests
                     &num_completed,                              // outcount
                     indices_buf.data(),                          // indices
                     statuses_buf.data());                        // array_of_statuses
        if (num_completed == 0 || num_completed == MPI_UNDEFINED) {
            unstep_probe_recursion();
            return false;
        }
        auto indices = std::span(indices_buf).first(num_completed);
        auto statuses = std::span(statuses_buf).first(num_completed);
        auto buffers = indices | std::views::transform([&](int index) -> auto& { return receive_buffers_[index]; });
        auto requests = indices | std::views::transform([&](int index) -> auto& { return receive_requests_[index]; });
#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto [buffer, status, request] : views::zip(buffers, statuses, requests)) {
            termination_->track_receive();
            auto envelope = internal::build_envelope(buffer, status, rank_);
            on_message(std::move(envelope));
            post_receive(buffer, request);
        }
        unstep_probe_recursion();
        return true;
    }

    void resize_buffers(std::size_t new_size, MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        std::vector<MPI_Status> statuses(receive_requests_.size());
        for (MPI_Request& request : receive_requests_) {
            MPI_Cancel(&request);
        }
        MPI_Waitall(static_cast<int>(receive_requests_.size()), receive_requests_.data(), statuses.data());
#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto&& [buffer, request, status] : views::zip(receive_buffers_, receive_requests_, statuses)) {
            int cancelled = 0;
            MPI_Test_cancelled(&status, &cancelled);
            if (!cancelled) {
                termination_->track_receive();
                auto envelope = build_envelope(buffer, status, rank_);
                on_message(std::move(envelope));
            }
            buffer.resize(new_size);
            post_receive(buffer, request);
        }
    }

    [[nodiscard]] std::size_t buffer_size() const {
        return receive_buffers_.front().size();
    }

private:
    void post_receive(ReceiveBufferContainer& buffer, MPI_Request& request) {
#if MPI_VERSION >= 4
        MPI_Irecv_c(buffer.data(),                        // buf
                    buffer.size(),                        // count
                    kamping::mpi_datatype<value_type>(),  // datatype
                    MPI_ANY_SOURCE,                       // source
                    tag_,                                 // tag
                    comm_,                                // comm
                    &request                              // request
        );
#else
        MPI_Irecv(buffer.data(),                        // buf
                  static_cast<int>(buffer.size()),      // count
                  kamping::mpi_datatype<value_type>(),  // datatype
                  MPI_ANY_SOURCE,                       // source
                  tag_,                                 // tag
                  comm_,                                // comm
                  &request                              // request
        );
#endif
    }

    auto step_probe_recursion() -> std::tuple<std::vector<MPI_Status>&, std::vector<int>&> {
        probe_recursion_depth_++;
        if (probe_recursion_depth_ >= static_cast<int>(statuses_.size())) {
            statuses_.emplace_back(receive_requests_.size());
            indices_.emplace_back(receive_requests_.size());
        }
        return {statuses_[probe_recursion_depth_], indices_[probe_recursion_depth_]};
    }

    auto unstep_probe_recursion() -> void {
        probe_recursion_depth_--;
    }

    MPI_Comm comm_;
    int tag_;
    std::vector<MPI_Request> receive_requests_;
    std::vector<ReceiveBufferContainer> receive_buffers_;
    std::vector<std::vector<MPI_Status>> statuses_;
    std::vector<std::vector<int>> indices_;
    int probe_recursion_depth_ = 0;
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
          termination_(&termination_counter) {
        KASSERT(tag < kamping::mpi_env.tag_upper_bound());
        MPI_Comm_rank(comm_, &rank_);
        for (ReceiveBufferContainer& buffer : receive_buffers_) {
            buffer.resize(reserved_receive_buffer_size);
        }
    }

    void rebind_termination_counter(internal::TerminationCounter& termination_counter) {
        termination_ = &termination_counter;
    }

    bool probe_for_one_message(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        if (slots_in_use_ >= receive_buffers_.size()) {
            return false;
        }
        MPI_Message message = MPI_MESSAGE_NULL;
        MPI_Status status;
        int probe_successful = 0;
        MPI_Improbe(MPI_ANY_SOURCE, tag_, comm_, &probe_successful, &message, &status);
        if (!probe_successful) {
            return false;
        }
        // Reserve the slot before calling on_message. handle_overflow inside on_message fires before merge() reads the
        // buffer (see post_message_impl), so without this, a recursive probe call would overwrite our slot.
        std::size_t my_slot = slots_in_use_++;
        auto& buffer = receive_buffers_[my_slot];
        MPI_Mrecv_c(buffer.data(), buffer.size(), kamping::mpi_datatype<value_type>(), &message, &status);
        termination_->track_receive();
        auto envelope = internal::build_envelope(buffer, status, rank_);
        on_message(std::move(envelope));
        slots_in_use_--;
        return true;
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message) {
        return probe_for_messages(std::forward<decltype(on_message)>(on_message), receive_buffers_.size());
    }

    bool probe_for_messages(MessageHandler<value_type, std::span<value_type>> auto&& on_message,
                            std::size_t max_receives) {
        // Mirrors PersistentReceiver's loop: for each completion { on_message(envelope); MPI_Start(&request); }
        // PersistentReceiver's MPI_Start re-opens the slot AFTER on_message so the slot is inactive (not receivable)
        // during the handler, preventing inner Testsome calls from completing it and aliasing the buffer.
        // Here, slots_in_use_++ before on_message / slots_in_use_-- after on_message plays the same role:
        // the slot is reserved for the duration of on_message; a recursive probe_for_one_message call sees it as
        // occupied and uses the next free slot instead. Processing one message at a time (rather than batching all
        // receives before any handler call) is essential: batching N messages locks all N slots simultaneously,
        // leaving none for recursive probes and causing a near-deadlock when second_hop send slots are exhausted.
        bool received_any = false;
        for (std::size_t i = 0; i < max_receives; i++) {
            if (!probe_for_one_message(std::forward<decltype(on_message)>(on_message))) {
                break;
            }
            received_any = true;
        }
        return received_any;
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
    internal::TerminationCounter* termination_;
    int rank_ = 0;
    // Number of slots currently held by an active probe call. Prevents recursive probes (via the progress_hook inside
    // handle_overflow) from reusing slots whose buffers are still referenced by a span in an outer on_message call.
    std::size_t slots_in_use_ = 0;
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

    void rebind_termination_counter(internal::TerminationCounter& termination_counter) {
        termination_ = &termination_counter;
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
#if MPI_VERSION >= 4
        MPI_Count count = 0;
        MPI_Get_count_c(&status, kamping::mpi_datatype<value_type>(), &count);
#else
        int count = 0;
        MPI_Get_count(&status, kamping::mpi_datatype<value_type>(), &count);
#endif
        buffer.resize(count);
#if MPI_VERSION >= 4
        MPI_Mrecv_c(buffer.data(), buffer.size(), kamping::mpi_datatype<value_type>(), &message, &status);
#else
        MPI_Mrecv(buffer.data(), static_cast<int>(buffer.size()), kamping::mpi_datatype<value_type>(), &message,
                  &status);
#endif
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
#ifdef BRIEFKASTEN_CXX20
        namespace views = ranges::views;
#else
        namespace views = std::views;
#endif
        for (auto [buffer, status] : views::zip(receive_buffers_, statuses_)) {
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

}  // namespace briefkasten
