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

namespace briefkasten {

static constexpr std::size_t DEFAULT_NUM_REQUEST_SLOTS = 8;
static constexpr std::size_t DEFAULT_BUFFER_THRESHOLD = 32ULL * 1024;

enum class FlushStrategy : std::uint8_t { local, global, random, largest };

struct Config {
    size_t num_request_slots = DEFAULT_NUM_REQUEST_SLOTS;
    size_t max_num_aggregation_buffers = 2 * DEFAULT_NUM_REQUEST_SLOTS;
    FlushStrategy flush_strategy = FlushStrategy::local;
    size_t global_threshold_bytes = std::numeric_limits<size_t>::max();
    std::size_t local_threshold_bytes = DEFAULT_BUFFER_THRESHOLD;
    std::size_t send_backlog_capacity = 0;
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
                         Config const& config,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{})
        : config_(config),
          queue_(comm, config_.num_request_slots, compute_buffer_size(config_), config_.send_backlog_capacity),
          local_threshold_bytes_(config_.local_threshold_bytes),
          global_threshold_bytes_(config_.global_threshold_bytes),
          max_num_aggregation_buffers_(config_.max_num_aggregation_buffers),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)),
          flush_strategy_(config_.flush_strategy) {
        reserve_aggregation_buffers(config_.num_request_slots);
    }

    ~BufferedMessageQueue() = default;
    BufferedMessageQueue(BufferedMessageQueue&&) = default;
    BufferedMessageQueue(BufferedMessageQueue const&) = delete;
    BufferedMessageQueue& operator=(BufferedMessageQueue&&) = default;
    BufferedMessageQueue& operator=(BufferedMessageQueue const&) = delete;

    /// Post a message to the queue. This operation never fails, but this requires to busily wait for completion of
    /// other send/receives until slots or buffers become available. Therefore, you also have to pass a message handler.
    ///
    /// The optional \p progress_hook is invoked on every iteration of the busy-wait loops. It allows the caller to
    /// drive progress on resources outside of this queue (e.g. a sibling queue in an indirection setup) while we are
    /// blocked waiting for one of our own sends to complete. Without this, two interdependent queues can deadlock,
    /// each spinning on its own sends while starving the other's receiver.
    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,
                               PEID envelope_sender,
                               PEID envelope_receiver,
                               int tag,
                               MessageHandler<MessageType> auto&& on_message,
                               std::invocable<> auto&& progress_hook) {
        auto ret = post_message_impl(
            std::forward<decltype(message)>(message), receiver, envelope_sender, envelope_receiver, tag,

            [&](auto it) {  // handle_overflow
                resolve_overflow_blocking(it, on_message, progress_hook);
            },
            [&] {  // get_new_buffer
                while (true) {
                    // try to get a free buffer and poll until one becomes available
                    auto buf = acquire_buffer();
                    if (buf.has_value()) {
                        return std::move(*buf);
                    }
                    poll(on_message);
                    progress_hook();
                }
            });
        return ret;
    }

    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,
                               PEID envelope_sender,
                               PEID envelope_receiver,
                               int tag,
                               MessageHandler<MessageType> auto&& on_message) {
        return post_message_blocking(std::forward<decltype(message)>(message), receiver, envelope_sender,
                                     envelope_receiver, tag, std::forward<decltype(on_message)>(on_message), [] {});
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
                auto buf = acquire_buffer();
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
    /// \return true if the buffer had some data to flush and succeeded, false otherwise
    bool flush_buffer(PEID receiver) {
        auto it = aggregation_buffers_.find(receiver);
        if (it != aggregation_buffers_.end()) {
            // bool buffer_was_empty = it->second.empty();
            auto new_it = flush_buffer_impl(it);
            return new_it.second;
            // if (new_it == it) {
            //   return false;
            // }
            // return buffer_was_empty;
        }
        return false;
    }

    void flush_all_buffers() {
        flush_all_aggregation_buffers_impl(aggregation_buffers_.end(), [] {}, [] { return false; });
    }

    void flush_largest_buffer() {
        std::ignore = flush_largest_buffer_impl(aggregation_buffers_.end());
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The
    /// Envelope (not necessarily the underlying data) is moved to the handler
    /// when called.
    auto poll(MessageHandler<MessageType> auto&& on_message) -> std::optional<std::pair<bool, bool>> {
        return queue_.poll(split_handler(on_message), [&](std::size_t receipt, BufferContainer buffer) {
            reclaim_aggregation_buffer(receipt, std::move(buffer));
        });
    }

    auto poll_throttled(MessageHandler<MessageType> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD) {
        return queue_.poll_throttled(
            split_handler(on_message),
            [&](std::size_t receipt, BufferContainer buffer) {
                reclaim_aggregation_buffer(receipt, std::move(buffer));
            },
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
        return terminate(
            std::forward<decltype(on_message)>(on_message), std::forward<decltype(progress_hook)>(progress_hook),
            [] { return internal::MessageCounter{.send = 0, .receive = 0}; }, [] {});
    }

    /// Termination variant that participates in a joint, multi-hop termination protocol.
    ///
    /// \p additional_counts contributes a sibling queue's send/receive counts to this queue's counting round, so a
    /// single allreduce decides termination for the whole system. \p extra_round_prepare runs once per counting round
    /// (right after flushing our own buffers, before the counts are snapshotted) and is where the sibling's buffers
    /// must be flushed so no buffered message stays invisible to the count.
    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message,
                                 std::invocable<> auto&& progress_hook,
                                 std::invocable<> auto&& additional_counts,
                                 std::invocable<> auto&& extra_round_prepare) {
        auto before_next_message_counting_round_hook = [&] {
            flush_all_buffers_blocking(on_message, [&] { return termination_state() == TerminationState::active; });
            extra_round_prepare();
        };
        bool ret = queue_.terminate(
            split_handler(on_message),
            [&](std::size_t receipt, BufferContainer buffer) {
                reclaim_aggregation_buffer(receipt, std::move(buffer));
            },
            before_next_message_counting_round_hook, progress_hook, additional_counts);
        return ret;
    }

    [[nodiscard]] internal::MessageCounter message_counts() const {
        return queue_.message_counts();
    }

    /// Flush every aggregation buffer, blocking only while send slots are actually exhausted.
    ///
    /// Unlike a wait-for-send-completion drain, this makes progress even when there are no outstanding sends, so it
    /// cannot deadlock on bounded buffer capacities when a buffer holds freshly aggregated, not-yet-sent data (e.g. a
    /// just-redirected message with a free send slot but nothing in flight).
    ///
    /// Each iteration first polls (non-blocking) so that incoming messages are observed: this both progresses sends
    /// and lets \p should_stop fire as soon as new work arrives, so we stop force-flushing not-yet-full buffers (which
    /// would defeat aggregation) once the termination attempt is going to be cancelled anyway.
    void flush_all_buffers_blocking(MessageHandler<MessageType> auto&& on_message,
                                    std::predicate auto&& should_stop) {
        auto it = aggregation_buffers_.begin();
        while (it != aggregation_buffers_.end()) {
            poll(on_message);  // observe arrivals (may flip should_stop) and progress sends
            if (should_stop()) {
                return;
            }
            while (!queue_.has_send_capacity()) {
                poll(on_message);  // only block when slots are exhausted; polling frees them as peers receive
                if (should_stop()) {
                    return;
                }
            }
            bool flushed = false;
            std::tie(it, flushed) = flush_buffer_impl(it, /*erase=*/true);
            KASSERT(flushed, "Flush must succeed once send capacity is ensured.");
        }
    }

    void reactivate() {
        queue_.reactivate();
    }

    [[nodiscard]] TerminationState termination_state() const {
        return queue_.termination_state();
    }

    bool progress_sending() {
        return queue_.progress_sending([&](std::size_t receipt, BufferContainer buffer) {
            reclaim_aggregation_buffer(receipt, std::move(buffer));
        });
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
            resolve_overflow_blocking(on_message, [] {});
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
        for (auto current = aggregation_buffers_.begin(); current != aggregation_buffers_.end(); current++) {
            if (check_for_local_buffer_overflow(current->second, 0)) {
                resolve_overflow_blocking(current, on_message, [] {});
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

    [[nodiscard]] Config const& config() const {
        return config_;
    }

    /// Raise (or lower) the cap on concurrently held aggregation buffers. The cap only bounds lazy growth in
    /// acquire_buffer(), so adjusting it after construction is safe; already-allocated buffers are untouched.
    void max_num_aggregation_buffers(std::size_t new_max) {
        max_num_aggregation_buffers_ = new_max;
        config_.max_num_aggregation_buffers = new_max;
    }

    [[nodiscard]] std::size_t max_num_aggregation_buffers() const {
        return max_num_aggregation_buffers_;
    }

    /// Adjust the underlying send backlog capacity at runtime (see Sender::set_send_backlog_capacity).
    void send_backlog_capacity(std::size_t new_capacity) {
        config_.send_backlog_capacity = new_capacity;
        queue_.set_send_backlog_capacity(new_capacity);
    }

    [[nodiscard]] std::size_t send_backlog_capacity() const {
        return config_.send_backlog_capacity;
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

    auto num_allocated_buffers() {
        return num_aggregation_buffers_;
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

    void reserve_aggregation_buffers(std::size_t num_buffers) {
        auto buffer_size = queue_.reserved_receive_buffer_size();
        reserve_aggregation_buffers(num_buffers, buffer_size);
    }

    // NOLINTNEXTLINE(*-easily-swappable-parameters)
    void reserve_aggregation_buffers(std::size_t num_buffers, std::size_t buffer_size) {
        if (num_aggregation_buffers_ + num_buffers > max_num_aggregation_buffers_) {
            throw std::runtime_error("Exceeded maximum number of aggregation buffers.");
        }
        auto old_size = free_aggregation_buffers_.size();
        free_aggregation_buffers_.resize(old_size + num_buffers);
        for (auto& buf :
             std::ranges::subrange(free_aggregation_buffers_.begin() + old_size, free_aggregation_buffers_.end())) {
            num_aggregation_buffers_++;
            buf.reserve(buffer_size);
        }
    }

    auto acquire_buffer() -> std::optional<BufferContainer> {
        if (free_aggregation_buffers_.empty()) {
            if (num_aggregation_buffers_ < max_num_aggregation_buffers_) {
                reserve_aggregation_buffers(1);
            } else {
                // Heuristic: at quota with no free buffer → flush one.
                // It won’t free capacity immediately, but once the send
                // completes the buffer will be recycled via reclaim_aggregation_buffer
                if (aggregation_buffers_.size() >= max_num_aggregation_buffers_) {
                    flush_largest_buffer();
                }
                return std::nullopt;
            }
        }
        KASSERT(!free_aggregation_buffers_.empty());
        auto buffer = std::move(free_aggregation_buffers_.back());
        free_aggregation_buffers_.pop_back();
        return buffer;
    };

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message_impl(InputMessageRange<MessageType> auto&& message,
                           PEID receiver,  // NOLINT(*-easily-swappable-parameters)
                           PEID envelope_sender,
                           PEID envelope_receiver,
                           int tag,
                           OverflowHandler<BufferMap> auto&& handle_overflow,
                           BufferProvider<BufferContainer> auto&& get_new_buffer) {
        auto it = aggregation_buffers_.find(receiver);
        if (it == aggregation_buffers_.end()) {
            auto buffer = get_new_buffer();
            std::tie(it, std::ignore) = aggregation_buffers_.emplace(receiver, std::move(buffer));
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

    /// @return an iterator to the next buffer (and true), or the input iterator (and false) if flushing failed
    auto flush_buffer_impl(BufferMap::iterator buffer_it, bool erase = true)
        -> std::pair<typename BufferMap::iterator, bool> {
        KASSERT(buffer_it != aggregation_buffers_.end(), "Trying to flush non-existing buffer.");
        auto& [receiver, buffer] = *buffer_it;
        if (buffer.empty()) {
            if (erase) {
                return {aggregation_buffers_.erase(buffer_it), true};
            }
            return {++buffer_it, true};
        }
        auto pre_cleanup_buffer_size = buffer.size();
        pre_send_cleanup(buffer, receiver);
        // we don't send if the cleanup has emptied the buffer
        if (buffer.empty()) {
            global_buffer_size_ -= pre_cleanup_buffer_size;
            if (erase) {
                BufferContainer container = std::move(buffer_it->second);
                auto next = aggregation_buffers_.erase(buffer_it);
                free_aggregation_buffers_.emplace_back(std::move(container));
                return {next, true};
            }
            return {++buffer_it, true};
        }
        if (!queue_.has_send_capacity()) {
            return {buffer_it, false};
        }
        auto receipt = queue_.post_message(std::move(buffer_it->second), receiver);
        KASSERT(receipt.has_value(),
                "We checked before that there is capacity, so posting the message should not fail.");
        global_buffer_size_ -= pre_cleanup_buffer_size;
        if (erase) {
            return {aggregation_buffers_.erase(buffer_it), true};
        }
        return {++buffer_it, true};
    }

    /// if post_flush_hook return true, this breaks the loop
    template <typename PreFlushHook, typename PostFlushHook>
        requires std::invocable<PreFlushHook> && (std::predicate<PostFlushHook> || std::predicate<PostFlushHook, bool>)
    bool flush_all_aggregation_buffers_impl(
        BufferMap::iterator current_buffer,
        PreFlushHook&& pre_flush_hook,    // NOLINT(cppcoreguidelines-missing-std-forward)
        PostFlushHook&& post_flush_hook,  // NOLINT(cppcoreguidelines-missing-std-forward)
        bool break_when_flush_fails = true) {
        auto it = aggregation_buffers_.begin();
        bool flushed_something = false;
        while (it != aggregation_buffers_.end()) {
            pre_flush_hook();
            bool current_flush_successful = false;
            std::tie(it, current_flush_successful) =
                flush_buffer_impl(it, it != current_buffer);  // iterator `it` is updated by std::tie; do not use its
                                                              // previous value after this call
            if (current_flush_successful) {
                flushed_something = true;
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
        auto largest_buffer =
            std::max_element(aggregation_buffers_.begin(), aggregation_buffers_.end(),
                             [](auto& lhs, auto& rhs) { return lhs.second.size() < rhs.second.size(); });
        if (largest_buffer != aggregation_buffers_.end()) {
            auto it = flush_buffer_impl(largest_buffer, largest_buffer != current_buffer);
            return it.second;
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

    auto reclaim_aggregation_buffer(std::size_t /*receipt*/, BufferContainer&& buffer) {
        buffer.resize(0);  // this does not reduce the capacity
        free_aggregation_buffers_.emplace_back(std::move(buffer));
    }

    /// @return returns false iff resolve failed
    bool resolve_overflow(BufferMap::iterator current_buffer) {
        switch (flush_strategy_) {
            case FlushStrategy::local: {
                auto ret = flush_buffer_impl(current_buffer, /*erase=*/false);
                return ret.second;
            }
            case FlushStrategy::global: {
                return flush_all_aggregation_buffers_impl(current_buffer, [] {}, [] { return false; });
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

    void resolve_overflow_blocking(BufferMap::iterator current_buffer,
                                   MessageHandler<MessageType> auto&& on_message,
                                   std::invocable<> auto&& progress_hook) {
        while (true) {
            auto res = poll(std::forward<decltype(on_message)>(on_message));
            if (res && res->first) {  // finished some send
                break;
            }
            progress_hook();
        }
        // now actually resolve the overflow
        bool success = resolve_overflow(current_buffer);
        if (success) {
            return;
        }
        throw std::runtime_error("Failed to resolve overflow in post_message_blocking. This should not happen.");
    }
    void resolve_overflow_blocking(MessageHandler<MessageType> auto&& on_message,
                                   std::invocable<> auto&& progress_hook) {
        resolve_overflow_blocking(aggregation_buffers_.end(), std::forward<decltype(on_message)>(on_message),
                                  std::forward<decltype(progress_hook)>(progress_hook));
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

    Config config_;
    MessageQueue<BufferType, BufferContainer, ReceiveBufferContainer> queue_;
    BufferMap aggregation_buffers_;
    BufferList free_aggregation_buffers_;
    size_t local_threshold_bytes_;
    size_t global_threshold_bytes_;
    std::size_t max_num_aggregation_buffers_;

    std::size_t num_aggregation_buffers_ = 0;

    Merger merge;
    Splitter split;
    BufferCleaner pre_send_cleanup;
    size_t global_buffer_size_ = 0;

    FlushStrategy flush_strategy_;
};
}  // namespace briefkasten
