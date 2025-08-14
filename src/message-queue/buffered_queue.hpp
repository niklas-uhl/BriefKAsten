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
#include <cstdint>
#include <kassert/kassert.hpp>
#include <limits>
#include <ranges>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "./aggregators.hpp"
#include "./detail/concepts.hpp"
#include "./detail/queue.hpp"

namespace message_queue {

static constexpr std::size_t DEFAULT_NUM_REQUEST_SLOTS = 8;
static constexpr std::size_t DEFAULT_BUFFER_THRESHOLD = 32ULL * 1024;

enum class FlushStrategy : std::uint8_t { local, global, random, largest };

struct Config {
    size_t num_request_slots = DEFAULT_NUM_REQUEST_SLOTS;
    size_t max_num_send_buffers = 2 * DEFAULT_NUM_REQUEST_SLOTS;
    FlushStrategy flush_strategy = FlushStrategy::local;
    size_t global_threshold_bytes = std::numeric_limits<size_t>::max();
    std::size_t local_threshold_bytes = DEFAULT_BUFFER_THRESHOLD;
    std::size_t out_buffer_capacity = 0;
};

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer<BufferType> BufferContainer = std::vector<BufferType>,
          MPIBuffer<BufferType> ReceiveBufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageType, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
class BufferedMessageQueue {
public:
    using message_type = MessageType;
    using buffer_type = BufferType;
    using buffer_container_type = BufferContainer;
    using merger_type = Merger;
    using splitter_type = Splitter;
    using buffer_cleaner_type = BufferCleaner;

    BufferedMessageQueue(MPI_Comm comm,
                         Config const config,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{})
        : queue_(comm, config.num_request_slots, compute_buffer_size(config), config.out_buffer_capacity),
          local_threshold_bytes_(config.local_threshold_bytes),
          global_threshold_bytes_(config.global_threshold_bytes),
          max_num_send_buffers_(config.max_num_send_buffers),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)),
          flush_strategy_(config.flush_strategy) {
        reserve_send_buffers(config.num_request_slots);
    }

    ~BufferedMessageQueue() = default;
    BufferedMessageQueue(BufferedMessageQueue&&) = default;
    BufferedMessageQueue(BufferedMessageQueue const&) = delete;
    BufferedMessageQueue& operator=(BufferedMessageQueue&&) = default;
    BufferedMessageQueue& operator=(BufferedMessageQueue const&) = delete;

    /// Post a message to the queue. This operation never fails, but this requires to busily wait for completion of
    /// other send/receives until slots or buffers become available. Therefore, you also have to pass a message handler.
    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,
                               PEID envelope_sender,
                               PEID envelope_receiver,
                               int tag,
                               MessageHandler<MessageType> auto&& on_message) {
        auto ret = post_message_impl(
            std::forward<decltype(message)>(message), receiver, envelope_sender, envelope_receiver, tag,

            [&](auto it) {  // handle_overflow
                resolve_overflow_blocking(it, on_message);
            },
            [&] {  // get_new_buffer
                while (true) {
                    // try to get a free buffer and poll until one becomes available
                    auto buf = get_some_free_buffer();
                    if (buf.has_value()) {
                        return std::move(*buf);
                    }
                    poll(on_message);
                }
            });
        return ret;
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0) {
        return post_message_blocking(std::forward<decltype(message)>(message), receiver, rank(), receiver, tag,
                                     std::forward<decltype(on_message)>(on_message));
    }

    bool post_message_blocking(MessageType message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0) {
        return post_message_blocking(std::ranges::views::single(message), receiver,
                                     std::forward<decltype(on_message)>(on_message), tag);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    ///
    /// if the message box capacity of the underlying queue is bounded, than this may fail and throw an exception
    bool post_message(InputMessageRange<MessageType> auto&& message,
                      PEID receiver,
                      PEID envelope_sender,
                      PEID envelope_receiver,
                      int tag) {
        return post_message_impl(
            std::forward<decltype(message)>(message), receiver, envelope_sender, envelope_receiver, tag,
            [&](auto it) {
                bool success = resolve_overflow(it);
                if (!success) {
                    throw std::runtime_error(
                        "Failed to resolve overflow, because sending to the underlying queue failed.");
                }
            },
            [&] {
                auto buf = get_some_free_buffer();
                if (!buf.has_value()) {
                    throw std::runtime_error("Failed to resolve overflow, because no free buffer was available.");
                }
                return std::move(*buf);
            });
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view
    ////
    /// if the message box capacity of the underlying queue is bounded, than this may fail and throw an exception
    bool post_message(InputMessageRange<MessageType> auto&& message, PEID receiver, int tag = 0) {
        return post_message(std::forward<decltype(message)>(message), receiver, rank(), receiver, tag);
    }

    bool post_message(MessageType message, PEID receiver, int tag = 0) {
        return post_message(std::ranges::views::single(message), receiver, tag);
    }

    /// Flush buffer for \p receiver. If the buffer is empty, or does not exist, this is a no-op.
    /// \param receiver The rank of the receiver
    /// \return true if the buffer had some data to flush, false otherwise
    bool flush_buffer(PEID receiver) {
        auto it = buffers_.find(receiver);
        if (it != buffers_.end()) {
            bool buffer_was_empty = it->second.empty();
            auto new_it = flush_buffer_impl(it);
            // if (new_it == it) {
            //   return false;
            // }
            return buffer_was_empty;
        }
        return false;
    }

    void flush_all_buffers() {
        flush_all_buffers_impl(buffers_.end(), [] {}, [] { return false; });
    }

    void flush_largest_buffer() {
        std::ignore = flush_largest_buffer_impl(buffers_.end());
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The
    /// Envelope (not necessarily the underlying data) is moved to the handler
    /// when called.
    auto poll(MessageHandler<MessageType> auto&& on_message) -> std::optional<std::pair<bool, bool>> {
        return queue_.poll(split_handler(on_message), [&](std::size_t receipt, BufferContainer buffer) {
            recover_buffer(receipt, std::move(buffer));
        });
    }

    auto poll_throttled(MessageHandler<MessageType> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD) {
        return queue_.poll_throttled(
            split_handler(on_message),
            [&](std::size_t receipt, BufferContainer buffer) { recover_buffer(receipt, std::move(buffer)); },
            poll_skip_threshold);
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message) {
        return terminate(std::forward<decltype(on_message)>(on_message), []() {});
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message, std::invocable<> auto&& progress_hook) {
        auto before_next_message_counting_round_hook = [&] {
            flush_all_buffers_and_poll_until_reactivated(on_message);
        };
        bool ret = queue_.terminate(
            split_handler(on_message),
            [&](std::size_t receipt, BufferContainer buffer) { recover_buffer(receipt, std::move(buffer)); },
            before_next_message_counting_round_hook, progress_hook);
        return ret;
    }

    void reactivate() {
        queue_.reactivate();
    }

    [[nodiscard]] TerminationState termination_state() const {
        return queue_.termination_state();
    }

    bool progress_sending() {
        return queue_.progress_sending(
            [&](std::size_t receipt, BufferContainer buffer) { recover_buffer(receipt, std::move(buffer)); });
    }

    bool probe_for_messages(MessageHandler<MessageType> auto&& on_message) {
        return queue_.probe_for_messages(split_handler(on_message));
    }

    /// on_message may be called multiple times, because this receives a whole buffer and applies the splitter to it
    bool probe_for_one_message(MessageHandler<MessageType> auto&& on_message,
                               PEID source = MPI_ANY_SOURCE,
                               int tag = MPI_ANY_TAG) {
        return queue_.probe_for_one_message(split_handler(on_message), source, tag);
    }

    [[nodiscard]] size_t global_threshold_bytes() const {
        return global_threshold_bytes_;
    }

    void global_threshold_bytes(std::size_t new_threshold, MessageHandler<MessageType> auto&& on_message) {
        Config config;
        config.global_threshold_bytes = new_threshold;
        global_threshold_bytes_ = new_threshold;
        if (check_for_global_buffer_overflow(0)) {
            // it's fine to send out message here, since we only grow buffers
            // so this will fit into buffer on even we resizing is not synchronized
            resolve_overflow_blocking(on_message);
        }
        auto new_buffer_size = compute_buffer_size(config);
        if (new_buffer_size > queue_.reserved_receive_buffer_size()) {
            // we need to resize the buffers, and catch potential stale messages
            queue_.resize_receive_buffers(new_buffer_size, split_handler(on_message));
            // no need to resize send buffers, they will grow while merging
            // newly allocated buffers will have the right size
        }  // otherwise we can just continue using the already allocated buffers
    }

    void local_threshold_bytes(std::size_t new_threshold, MessageHandler<MessageType> auto&& on_message) {
        Config config;
        config.local_threshold_bytes = new_threshold;
        local_threshold_bytes_ = new_threshold;
        for (auto current = buffers_.begin(); current != buffers_.end(); current++) {
            if (check_for_local_buffer_overflow(current->second, 0)) {
                resolve_overflow_blocking(current, on_message);
            }
        }
        auto new_buffer_size = compute_buffer_size(config);
        if (new_buffer_size > queue_.reserved_receive_buffer_size()) {
	  queue_.resize_receive_buffers(new_buffer_size, split_handler(on_message));
        }
    }

    [[nodiscard]] size_t local_threshold_bytes() const {
        return local_threshold_bytes_;
    }

    [[nodiscard]] PEID rank() const {
        return queue_.rank();
    }

    [[nodiscard]] PEID size() const {
        return queue_.size();
    }

    [[nodiscard]] MPI_Comm communicator() const {
        return queue_.communicator();
    }

    [[nodiscard]] auto& underlying() {
        return queue_;
    }

    /// if this mode is active, no incoming messages will cancel the termination process
    /// this allows using the queue as a somewhat async sparse-all-to-all
    void synchronous_mode(bool use_it = true) {
        queue_.synchronous_mode(use_it);
    }

private:
    using BufferMap = std::unordered_map<PEID, BufferContainer>;
    using BufferList = std::vector<BufferContainer>;

    static std::size_t compute_buffer_size(Config const& config) {
        if (config.local_threshold_bytes != std::numeric_limits<std::size_t>::max()) {
            return (config.local_threshold_bytes + sizeof(BufferType) - 1) / sizeof(BufferType);
        }
        if (config.global_threshold_bytes == std::numeric_limits<std::size_t>::max()) {
            return 0;
        }
        auto bytes_per_buffer = 2 * (config.global_threshold_bytes / config.num_request_slots);
        return (bytes_per_buffer + sizeof(BufferType) - 1) / sizeof(BufferType);
    }

    void reserve_send_buffers(std::size_t num_buffers) {
        auto buffer_size = queue_.reserved_receive_buffer_size();
        reserve_send_buffers(num_buffers, buffer_size);
    }

    void reserve_send_buffers(std::size_t num_buffers, std::size_t buffer_size) {
        if (num_send_buffers_ + num_buffers > max_num_send_buffers_) {
            throw std::runtime_error("Exceeded maximum number of send buffers.");
        }
        auto old_size = buffer_free_list_.size();
        buffer_free_list_.resize(old_size + num_buffers);
        for (auto& buf : std::ranges::subrange(buffer_free_list_.begin() + old_size, buffer_free_list_.end())) {
            num_send_buffers_++;
            buf.reserve(buffer_size);
        }
    }

    auto get_some_free_buffer() -> std::optional<BufferContainer> {
        if (buffer_free_list_.empty()) {
            if (num_send_buffers_ < max_num_send_buffers_) {
                reserve_send_buffers(1);
            } else {
                // Heuristic: at quota with no free buffer → flush one.
                // It won’t free capacity immediately, but once the send
                // completes the buffer will be recycled via recover_buffer
                if (buffers_.size() - 1 >= max_num_send_buffers_ /* minus the buffer we try to create */) {
                    flush_largest_buffer();
                }
                return std::nullopt;
            }
        }
        KASSERT(!buffer_free_list_.empty());
        auto buffer = std::move(buffer_free_list_.back());
        return buffer;
    };

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message_impl(InputMessageRange<MessageType> auto&& message,
                           PEID receiver,
                           PEID envelope_sender,
                           PEID envelope_receiver,
                           int tag,
                           OverflowHandler<BufferMap> auto&& handle_overflow,
                           BufferProvider<BufferContainer> auto&& get_new_buffer) {
        auto it = buffers_.find(receiver);
        if (it == buffers_.end()) {
            auto buffer = get_new_buffer();
            std::tie(it, std::ignore) = buffers_.emplace(receiver, std::move(buffer));
        }

        auto& buffer = it->second;
        auto envelope =
            MessageEnvelope{std::forward<decltype(message)>(message), envelope_sender, envelope_receiver, tag};
        size_t estimated_new_buffer_size = 0;
        if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
            estimated_new_buffer_size = merge.estimate_new_buffer_size(buffer, receiver, queue_.rank(), envelope);
        } else {
            estimated_new_buffer_size = buffer.size() + envelope.message.size();
        }
        auto old_buffer_size = buffer.size();
        bool overflow = false;
        if (check_for_buffer_overflow(buffer, estimated_new_buffer_size - old_buffer_size)) {
            overflow = true;
            handle_overflow(it);  // customization point
            buffer = get_new_buffer();
        }
        merge(buffer, receiver, queue_.rank(), std::move(envelope));
        auto new_buffer_size = buffer.size();
        global_buffer_size_ += new_buffer_size - old_buffer_size;
        return overflow;
    }

    void flush_all_buffers_and_poll_until_reactivated(MessageHandler<MessageType> auto&& on_message) {
        flush_all_buffers_impl(
            buffers_.end(),
            [&] {  // pre_flush_hook
                while (true) {
                    auto res = poll(std::forward<decltype(on_message)>(on_message));
                    if (res && res->first) {
                        break;
                    }
                }
            },
            [&](bool current_flush_successful) {  // post flush_hook
                if (!current_flush_successful) {
                    poll(std::forward<decltype(on_message)>(on_message));
                }
                return termination_state() == TerminationState::active;
            },
            false);
    }

    /// @return an iterator to the next buffer, or the input iterator if flushing failed
    auto flush_buffer_impl(BufferMap::iterator buffer_it, bool erase = true) -> BufferMap::iterator {
        KASSERT(buffer_it != buffers_.end(), "Trying to flush non-existing buffer.");
        auto& [receiver, buffer] = *buffer_it;
        if (buffer.empty()) {
            if (erase) {
                return buffers_.erase(buffer_it);
            }
            return ++buffer_it;
        }
        auto pre_cleanup_buffer_size = buffer.size();
        pre_send_cleanup(buffer, receiver);
        // we don't send if the cleanup has emptied the buffer
        if (buffer.empty()) {
            if (erase) {
                return buffers_.erase(buffer_it);
            }
            return ++buffer_it;
        }
        if (!queue_.has_send_capacity()) {
            return buffer_it;
        }
        auto receipt = queue_.post_message(std::move(buffer_it->second), receiver);
        KASSERT(receipt.has_value(),
                "We checked before that there is capacity, so posting the message should not fail.");
        global_buffer_size_ -= pre_cleanup_buffer_size;
        if (erase) {
            return buffers_.erase(buffer_it);
        }
        return ++buffer_it;
    }

    /// if post_flush_hook return true, this breaks the loop
    template <typename PreFlushHook, typename PostFlushHook>
        requires std::invocable<PreFlushHook> && (std::predicate<PostFlushHook> || std::predicate<PostFlushHook, bool>)
    bool flush_all_buffers_impl(BufferMap::iterator current_buffer,
                                PreFlushHook&& pre_flush_hook,    // NOLINT(cppcoreguidelines-missing-std-forward)
                                PostFlushHook&& post_flush_hook,  // NOLINT(cppcoreguidelines-missing-std-forward)
                                bool break_when_flush_fails = true) {
        auto it = buffers_.begin();
        bool flushed_something = false;
        while (it != buffers_.end()) {
            pre_flush_hook();
            bool current_flush_successful = false;
            auto new_it = flush_buffer_impl(it, it != current_buffer);
            if (new_it != it) {
                flushed_something = true;
                it = new_it;
                current_flush_successful = true;
            } else {
                if (break_when_flush_fails) {
                    return flushed_something;
                }
            }
            bool should_break = [&] {
                if constexpr (std::predicate<PostFlushHook>) {
                    return post_flush_hook();
                } else {
                    return post_flush_hook(current_flush_successful);
                }
            }();
            if (should_break) {
                return flushed_something;
            }
        }
        return flushed_something;
    }

    [[nodiscard]] bool flush_largest_buffer_impl(BufferMap::iterator current_buffer) {
        auto largest_buffer = std::max_element(buffers_.begin(), buffers_.end(), [](auto& lhs, auto& rhs) {
            return lhs.second.size() < rhs.second.size();
        });
        if (largest_buffer != buffers_.end()) {
            auto it = flush_buffer_impl(largest_buffer, largest_buffer != current_buffer);
            return it != largest_buffer;
        }
        return true;
    }

    auto split_handler(MessageHandler<MessageType> auto&& on_message) {
        return [&](Envelope<BufferType> auto buffer) {
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
        };
    }

    auto recover_buffer(std::size_t /*receipt*/, BufferContainer&& buffer) {
        buffer.resize(0);  // this does not reduce the capacity
        buffer_free_list_.emplace_back(std::move(buffer));
    }

    /// @return returns false iff resolve failed
    bool resolve_overflow(BufferMap::iterator current_buffer) {
        switch (flush_strategy_) {
            case FlushStrategy::local: {
                auto ret = flush_buffer_impl(current_buffer, /*erase=*/false);
                return ret != current_buffer;
            }
            case FlushStrategy::global: {
                return flush_all_buffers_impl(current_buffer, [] {}, [] { return false; });
            }
            case FlushStrategy::random: {
                throw std::runtime_error("Random flush strategy not implemented");
                return false;
            }
            case FlushStrategy::largest: {
                return flush_largest_buffer_impl(current_buffer);
            }
        }
        // unreachable
        return false;
    }

    void resolve_overflow_blocking(BufferMap::iterator current_buffer, MessageHandler<MessageType> auto&& on_message) {
        while (true) {
            auto res = poll(std::forward<decltype(on_message)>(on_message));
            if (res && res->first) {  // finished some send
                break;
            }
        }
        // now actually resolve the overflow
        bool success = resolve_overflow(current_buffer);
        if (success) {
            return;
        }
        throw std::runtime_error("Failed to resolve overflow in post_message_blocking. This should not happen.");
    }
    void resolve_overflow_blocking(MessageHandler<MessageType> auto&& on_message) {
        resolve_overflow_blocking(buffers_.end(), std::forward<decltype(on_message)>(on_message));
    }

    [[nodiscard]] bool check_for_global_buffer_overflow(std::uint64_t buffer_size_delta) const {
        if (global_threshold_bytes_ == std::numeric_limits<size_t>::max()) {
            return false;
        }
        return (global_buffer_size_ + buffer_size_delta) * sizeof(BufferType) > global_threshold_bytes_;
    }

    [[nodiscard]] bool check_for_local_buffer_overflow(BufferContainer const& buffer,
                                                       std::uint64_t buffer_size_delta) const {
        if (local_threshold_bytes_ == std::numeric_limits<size_t>::max()) {
            return false;
        }
        return (buffer.size() + buffer_size_delta) * sizeof(BufferType) > local_threshold_bytes_;
    }

    [[nodiscard]] bool check_for_buffer_overflow(BufferContainer const& buffer, std::uint64_t buffer_size_delta) const {
        return check_for_global_buffer_overflow(buffer_size_delta) ||
               check_for_local_buffer_overflow(buffer, buffer_size_delta);
    }

    MessageQueue<BufferType, BufferContainer, ReceiveBufferContainer> queue_;
    BufferMap buffers_;
    BufferList buffer_free_list_;
    size_t local_threshold_bytes_;
    size_t global_threshold_bytes_;
    std::size_t max_num_send_buffers_;

    std::size_t num_send_buffers_ = 0;

    Merger merge;
    Splitter split;
    BufferCleaner pre_send_cleanup;
    size_t global_buffer_size_ = 0;

    FlushStrategy flush_strategy_;
};
}  // namespace message_queue
