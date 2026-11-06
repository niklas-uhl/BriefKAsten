#pragma once

#include <mpi.h>
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <kamping/types/mpi_type_traits.hpp>
#include "debug_print.h"
#include "message-queue/debug_print.h"
#include "message-queue/message_statistics.h"

namespace message_queue {
template <typename T>
class MessageQueue {
    template <class U, class Merger, class Splitter>
    friend class BufferedMessageQueue;
    template <class U, class Merger, class Splitter>
    friend class ConcurrentBufferedMessageQueue;
    static_assert(kamping::types::has_static_type_v<T> && !kamping::types::mpi_type_traits<T>::has_to_be_committed,
                  "Only builtin MPI types are supported");

    enum class State { posted, initiated, completed };

    template <class S>
    struct MessageHandle {
        size_t message_size;
        std::vector<S> message;
        MPI_Request request;
        size_t request_id;
        int tag = MPI_ANY_TAG;
        State state = State::posted;

        bool test() {
            if (state == State::completed) {
                return true;
            }
            if (state == State::posted) {
                return false;
            }
            int finished = false;
            MPI_Status status;
            assert(state == State::initiated);
            assert(this->request != MPI_REQUEST_NULL);
            int err = MPI_Test(&request, &finished, &status);
            check_mpi_error(err, __FILE__, __LINE__);
            if (finished) {
                state = State::completed;
                return true;
            }
            return false;
        }
    };

    template <class S>
    using MessageHandlePtr = std::unique_ptr<MessageHandle<S>>;

    template <class S>
    struct SendHandle : MessageHandle<S> {
        PEID receiver = MPI_ANY_SOURCE;

        void initiate_send() {
            // atomic_debug(this->request_id);
            int err = MPI_Isend(this->message.data(), this->message.size(), kamping::types::mpi_type_traits<S>::data_type(),
                                receiver, this->tag, MPI_COMM_WORLD, &this->request);
            check_mpi_error(err, __FILE__, __LINE__);
            this->state = State::initiated;
        }
    };

    template <class S>
    using SendHandlePtr = std::unique_ptr<SendHandle<S>>;

    template <class S>
    struct ReceiveHandle : MessageHandle<S> {
#ifdef MESSAGE_QUEUE_MATCHED_RECV
        MPI_Message matched_message;
#endif
        MPI_Status status;
        PEID sender = MPI_ANY_SOURCE;

        void start_receive() {
            if (this->state != State::posted) {
                return;
            }
            this->state = State::initiated;
            this->message.resize(this->message_size);
#ifdef MESSAGE_QUEUE_MATCHED_RECV
            MPI_Imrecv(this->message.data(), this->message.size(), kamping::types::mpi_type_traits<S>::data_type(),
                       &matched_message, &(this->request));
#else
            MPI_Irecv(this->message.data(), this->message.size(), kamping::types::mpi_type_traits<S>::data_type(),
                      this->sender, this->tag, MPI_COMM_WORLD, &(this->request));
#endif
        }

        void receive() {
            if (this->state != State::posted) {
                return;
            }
            this->message.resize(this->message_size);
#ifdef MESSAGE_QUEUE_MATCHED_RECV
            MPI_Mrecv(this->message.data(), this->message.size(), kamping::types::mpi_type_traits<S>::data_type(),
                       &this->matched_message, MPI_STATUS_IGNORE);
#else
            MPI_Recv(this->message.data(), this->message.size(), kamping::types::mpi_type_traits<S>::data_type(), this->sender,
                     this->tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
#endif
            this->state = State::completed;
        }
    };

    template <class S>
    using ReceiveHandlePtr = std::unique_ptr<ReceiveHandle<S>>;

    struct ProbeResult {
        MPI_Status status;
        MPI_Message matched_message;
        PEID sender;
        int tag;

        template <typename S>
        ReceiveHandlePtr<S> handle() {
            auto handle = std::make_unique<ReceiveHandle<S>>();
            int message_size;
            MPI_Get_count(&status, kamping::types::mpi_type_traits<S>::data_type(), &message_size);
            handle->message_size = message_size;
#ifdef MESSAGE_QUEUE_MATCHED_RECV
            handle->matched_message = matched_message;
#endif
            handle->tag = status.MPI_TAG;
            handle->sender = status.MPI_SOURCE;
            return handle;
        }
    };

    static std::optional<ProbeResult> probe(PEID source = MPI_ANY_SOURCE, PEID tag = MPI_ANY_TAG) {
        ProbeResult result;
        int message_found = false;
#ifdef MESSAGE_QUEUE_MATCHED_RECV
        MPI_Improbe(source, tag, MPI_COMM_WORLD, &message_found, &result.matched_message, &result.status);
#else
        MPI_Iprobe(source, tag, MPI_COMM_WORLD, &message_found, &result.status);
#endif
        if (message_found) {
            result.sender = result.status.MPI_SOURCE;
            result.tag = result.status.MPI_TAG;
            return result;
        }
        return std::nullopt;
    }

    template <typename S>
    void post_message_impl(std::vector<SendHandlePtr<S>>& handles, std::vector<S>&& message, PEID receiver, int tag) {
        handles.emplace_back(new SendHandle<S>{});
        SendHandlePtr<S>& handle = handles.back();
        if (message.empty()) {
            std::abort();
        }
        handle->message = std::move(message);
        handle->receiver = receiver;
        handle->tag = tag;
        handle->request_id = this->request_id_;
        this->request_id_++;
        if (messages_in_transit_ < max_messages_in_transit_) {
            handle->initiate_send();
            messages_in_transit_++;
        }
    }

public:
    MessageQueue() : send_handles_(), recv_handles_(), stats_(), request_id_(0), rank_(0), size_(0) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
        MPI_Comm_size(MPI_COMM_WORLD, &size_);
    }

    void post_message(std::vector<T>&& message, PEID receiver, int tag = 0) {
        // assert(receiver != rank_);
        if (message.empty()) {
            return;
        }
        assert(tag != control_wave_tag);
        // std::cout << message.size() <<"\n";
        size_t message_size = message.size();
        post_message_impl(send_handles_, std::move(message), receiver, tag);
        stats_.sent_messages.fetch_add(1, std::memory_order_relaxed);
        stats_.send_volume.fetch_add(message_size, std::memory_order_relaxed);
    }

    template <typename MessageHandler>
    bool poll(MessageHandler&& on_message) {
        // atomic_debug("Inner poll");
        static_assert(std::is_invocable_v<MessageHandler, std::vector<T>, PEID>);
        auto check_and_remove [[maybe_unused]] = [&](auto& handles, auto on_request_finish) {
            size_t i = 0;
            while (i < handles.size()) {
                auto& handle = handles[i];
                if (handle->test()) {
                    on_request_finish(*handle);
                    if (i < handles.size() - 1) {
                        handles[i] = std::move(handles.back());
                    }
                    handles.resize(handles.size() - 1);
                } else {
                    i++;
                }
            }
        };
        bool something_happenend = false;
        size_t i = 0;
        while (i < send_handles_.size()) {
            auto& handle = send_handles_[i];
            if (handle->state == State::posted) {
                if (messages_in_transit_ < max_messages_in_transit_) {
                    handle->initiate_send();
                    messages_in_transit_++;
                }
            }
            if (handle->test()) {
                messages_in_transit_--;
                if (i < send_handles_.size() - 1) {
                    send_handles_[i] = std::move(send_handles_.back());
                }
                send_handles_.resize(send_handles_.size() - 1);
            } else {
                i++;
            }
        }
        while (true) {
            std::optional result = probe();
            if (!result.has_value()) {
                break;
            }
            something_happenend = true;
            auto handle = result.value().template handle<T>();
            handle->request_id = this->request_id_;
            this->request_id_++;
#ifdef MESSAGE_QUEUE_BLOCKING_RECEIVE
            handle->receive();
            stats_.received_messages.fetch_add(1, std::memory_order_relaxed);
            stats_.receive_volume.fetch_add(handle->message.size(), std::memory_order_relaxed);
            something_happenend = true;
            on_message(std::move(handle->message), handle->sender);
#else
            recv_handles_.emplace_back(std::move(handle));
            recv_handles_.back()->start_receive();
#endif
        }
#ifndef MESSAGE_QUEUE_BLOCKING_RECEIVE
        check_and_remove(recv_handles_, [&](ReceiveHandle<T>& handle) {
            stats_.received_messages.fetch_add(1, std::memory_order_relaxed);
            stats_.receive_volume.fetch_add(handle.message.size(), std::memory_order_relaxed);
            something_happenend = true;
            on_message(std::move(handle.message), handle.sender);
        });
#endif
        return something_happenend;
    }

    template <typename MessageHandler>
    void terminate(MessageHandler&& on_message) {
        terminate_impl(on_message, []() {});
    }

    const MessageStatistics& stats() {
        return stats_;
    }

    void reset() {
        send_handles_.clear();
        recv_handles_.clear();
        stats_ = MessageStatistics();
        request_id_ = 0;
        termination_state = TerminationState::active;
        number_of_waves = 0;
    }

    void reactivate() {
        termination_state = TerminationState::active;
    }

    PEID rank() const {
        return rank_;
    }

    PEID size() const {
        return size_;
    }

    template <typename MessageHandler, typename PreWaveHook>
    void terminate_impl(MessageHandler&& on_message, PreWaveHook&& pre_wave) {
        // atomic_debug("Inner terminate");
        std::pair<size_t, size_t> global_count = {0, 0};
        int wave_count = 0;
        while (true) {
            pre_wave();
            while (!send_handles_.empty() && !recv_handles_.empty()) {
                poll(on_message);
            }
            // atomic_debug("Handles empty");
            std::pair<size_t, size_t> local_count = {stats_.sent_messages, stats_.received_messages};
            std::pair<size_t, size_t> reduced_count;
            MPI_Request reduce_request;
            MPI_Iallreduce(&local_count, &reduced_count, 2, kamping::types::mpi_type_traits<size_t>::data_type(), MPI_SUM,
                           MPI_COMM_WORLD, &reduce_request);
            wave_count++;
            int reduce_finished = false;
            while (!reduce_finished) {
                poll(on_message);
                if (reduce_request == MPI_REQUEST_NULL) {
                    throw "Error";
                }
                int err = MPI_Test(&reduce_request, &reduce_finished, MPI_STATUS_IGNORE);
                if (err != MPI_SUCCESS) {
                    throw "Error";
                }
            }
            if (rank_ == 0) {
                // atomic_debug("Wave count " + std::to_string(wave_count));
            }
            if (reduced_count == global_count && global_count.first == global_count.second) {
                break;
            } else {
                global_count = reduced_count;
            }
        }
    }

    template <typename MessageHandler, typename PreWaveHook>
    bool try_terminate_impl(MessageHandler&& on_message, PreWaveHook&& pre_wave) {
        if (size_ == 1) {
            return true;
        }
        termination_state = TerminationState::trying_termination;
        int wave_count = 0;
        while (true) {
            pre_wave();
            while (!send_handles_.empty() && !recv_handles_.empty()) {
                poll(on_message);
                // atomic_debug("Poll before");
                if (termination_state == TerminationState::active) {
                    // atomic_debug("Reactivated");
                    return false;
                }
            }
            std::pair<size_t, size_t> reduced_count;
            if (termination_request == MPI_REQUEST_NULL) {
                local_count = {stats_.sent_messages, stats_.received_messages};
                // atomic_debug("Start reduce");
                MPI_Iallreduce(&local_count, &reduced_count, 2, kamping::types::mpi_type_traits<size_t>::data_type(), MPI_SUM,
                               MPI_COMM_WORLD, &termination_request);
            }
            wave_count++;
            int reduce_finished = false;
            while (!reduce_finished) {
                poll(on_message);
                // atomic_debug("Poll after inititated");
                int err = MPI_Test(&termination_request, &reduce_finished, MPI_STATUS_IGNORE);
                check_mpi_error(err, __FILE__, __LINE__);
                if (termination_state == TerminationState::active) {
                    // atomic_debug("Reactivated");
                    return false;
                }
            }
            // atomic_debug("Reduce finished");
            if (rank_ == 0) {
                // atomic_debug("Wave count " + std::to_string(wave_count));
            }
            if (reduced_count == global_count && global_count.first == global_count.second && global_count.first != 0) {
                // atomic_debug("Terminated");
                // atomic_debug(reduced_count);
                termination_state = TerminationState::terminated;
                return true;
            } else {
                global_count = reduced_count;
            }
        }
    }

private:
    enum class TerminationState { active, trying_termination, terminated };
    std::vector<SendHandlePtr<T>> send_handles_;
    std::vector<ReceiveHandlePtr<T>> recv_handles_;
    std::pair<size_t, size_t> local_count;
    std::pair<size_t, size_t> reduced_count;
    std::pair<size_t, size_t> global_count = {std::numeric_limits<size_t>::max(),
                                              std::numeric_limits<size_t>::max() - 1};
    MPI_Request termination_request = MPI_REQUEST_NULL;
    MessageStatistics stats_;
    int static const control_wave_tag = 478;
    size_t request_id_;
    PEID rank_;
    PEID size_;
    TerminationState termination_state = TerminationState::active;
    size_t number_of_waves = 0;
    size_t messages_in_transit_ = 0;
    size_t static const max_messages_in_transit_ = 10;
};

}  // namespace message_queue
