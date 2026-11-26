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
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <kamping/environment.hpp>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <kassert/kassert.hpp>
#include <limits>
#include <optional>
#include <ranges>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "./aggregators.hpp"
#include "./detail/concepts.hpp"
#include "./detail/flow_control.hpp"
#include "./detail/queue.hpp"

/// Stall tracer for debugging hangs: build with -DBRIEFKASTEN_STALL_TRACE=ON and set
/// BRIEFKASTEN_STALL_TRACE_SECONDS=5 to periodically dump the queue and flow-control state.
#ifdef BRIEFKASTEN_STALL_TRACE
#define BRIEFKASTEN_STALL_TRACE_TICK() stall_trace_tick()
#else
#define BRIEFKASTEN_STALL_TRACE_TICK() ((void)0)
#endif

namespace briefkasten {

static constexpr std::size_t DEFAULT_NUM_REQUEST_SLOTS = 8;
static constexpr std::size_t DEFAULT_BUFFER_THRESHOLD = 32ULL * 1024;
/// Credit window per peer, in buffers. 8 is the smallest value that did not cost performance.
static constexpr std::size_t DEFAULT_NUM_CREDIT_BUFFERS = 8;
/// Aggregation buffers per peer (one filling, one parked). The pool is shared, not reserved per peer.
static constexpr std::size_t DEFAULT_BUFFERS_PER_PEER = 2;

enum class FlushStrategy : std::uint8_t { local, global, random, largest };

struct Config {
    size_t num_request_slots = DEFAULT_NUM_REQUEST_SLOTS;
    std::optional<std::size_t> max_num_aggregation_buffers = std::nullopt;
    FlushStrategy flush_strategy = FlushStrategy::local;
    size_t global_threshold_bytes = std::numeric_limits<size_t>::max();
    std::size_t local_threshold_bytes = DEFAULT_BUFFER_THRESHOLD;
    std::optional<std::size_t> send_backlog_capacity = std::nullopt;
    /// Credit window per peer, in buffers. nullopt: DEFAULT_NUM_CREDIT_BUFFERS, 0: flow control off.
    std::optional<std::size_t> num_credit_buffers = std::nullopt;
    /// Aggregation buffer pool cap = buffers_per_peer * #peers + num_request_slots (only with flow control).
    /// nullopt: DEFAULT_BUFFERS_PER_PEER.
    std::optional<std::size_t> buffers_per_peer = std::nullopt;
};

/// Apply double-buffering defaults to \p config for a queue with at most \p fan_out distinct
/// destinations, leaving any field that was set explicitly untouched. With flow control on,
/// enable_flow_control overrides both fields.
///
///   send_backlog_capacity       = fan_out
///   max_num_aggregation_buffers = send_backlog_capacity + fan_out + num_request_slots
///
/// Buffers are allocated lazily, so sparse workloads pay only for their active destinations. For large
/// fan_out, startup overhead (MPI connection setup, NIC resources) grows with the number of distinct
/// partners — buffer sizing cannot address that. Use IndirectionAdapter to reduce live partners to
/// O(sqrt(p)) when startup overhead dominates.
inline Config apply_fan_out_defaults(Config config, std::size_t fan_out) {
    if (!config.send_backlog_capacity) {
        config.send_backlog_capacity = fan_out;
    }
    if (!config.max_num_aggregation_buffers) {
        config.max_num_aggregation_buffers =
            config.send_backlog_capacity.value() + fan_out + config.num_request_slots;
    }
    return config;
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer<BufferType> BufferContainer = std::vector<BufferType>,
          MPIBuffer<BufferType> ReceiveBufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageType, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner,
          template <typename> typename Receiver = PersistentReceiver>
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
        : user_config_(config),
          effective_config_(apply_comm_size_defaults(comm, user_config_)),
          queue_(comm, effective_config_.num_request_slots, compute_buffer_size(effective_config_),
                 effective_config_.send_backlog_capacity.value()),
          local_threshold_bytes_(effective_config_.local_threshold_bytes),
          global_threshold_bytes_(effective_config_.global_threshold_bytes),
          max_num_aggregation_buffers_(effective_config_.max_num_aggregation_buffers.value()),
          flow_(comm, kamping::Environment<>::tag_upper_bound() - 3, effective_config_.num_request_slots),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)),
          flush_strategy_(effective_config_.flush_strategy) {
        reserve_aggregation_buffers(effective_config_.num_request_slots);
#ifdef BRIEFKASTEN_STALL_TRACE
        if (char const* trace = std::getenv("BRIEFKASTEN_STALL_TRACE_SECONDS")) {
            stall_trace_interval_ = std::strtod(trace, nullptr);
            stall_trace_last_ = std::chrono::steady_clock::now();
        }
#endif
        // flow control is on by default; IndirectionAdapter re-configures it for its smaller peer count
        if (effective_config_.num_credit_buffers.value_or(DEFAULT_NUM_CREDIT_BUFFERS) > 0) {
            int comm_size = 0;
            MPI_Comm_size(comm, &comm_size);
            enable_flow_control(effective_config_.num_credit_buffers.value_or(DEFAULT_NUM_CREDIT_BUFFERS),
                                effective_config_.buffers_per_peer.value_or(DEFAULT_BUFFERS_PER_PEER),
                                static_cast<std::size_t>(comm_size));
        }
    }

    /// Give each of \p num_peers peers a credit window of \p num_credit_buffers buffers, and cap the buffer pool
    /// at \p buffers_per_peer per peer. \p num_credit_buffers must be the same on every rank, since the initial
    /// windows are implicit. The proxy may exceed the pool cap instead of blocking (see acquire_buffer).
    void enable_flow_control(std::size_t num_credit_buffers, std::size_t buffers_per_peer, std::size_t num_peers) {
        if (num_credit_buffers == 0) {
            return;
        }
        num_peers_ = num_peers;
        auto const buffer_size = std::max<std::size_t>(queue_.reserved_receive_buffer_size(), 1);
        flow_.configure(num_credit_buffers * buffer_size, num_peers);
        // buffers wait in per-destination parking queues instead of the sender's single FIFO backlog
        if (!user_config_.send_backlog_capacity) {
            queue_.set_send_backlog_capacity(0);
        }
        if (!user_config_.max_num_aggregation_buffers) {
            max_num_aggregation_buffers((buffers_per_peer * num_peers) +
                                        effective_config_.num_request_slots);
        }
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

            [&](auto /*it*/) {  // handle_overflow
                // by receiver, not iterator: polling may modify aggregation_buffers_
                resolve_overflow_blocking(receiver, on_message, progress_hook);
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
            [&](auto /*it*/) {
                bool success = resolve_overflow(receiver);
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
        BRIEFKASTEN_STALL_TRACE_TICK();
        flow_.poll();
        auto result = queue_.poll(split_handler(on_message), [&](std::size_t receipt, BufferContainer buffer) {
            reclaim_aggregation_buffer(receipt, std::move(buffer));
        });
        send_parked();
        return result;
    }

    /// Only every \p poll_skip_threshold-th call polls; this throttles grants and parked buffers too.
    auto poll_throttled(MessageHandler<MessageType> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD)
        -> std::optional<std::pair<bool, bool>> {
        if (poll_skip_threshold > 1 && (poll_throttle_count_++ % poll_skip_threshold) != 0) {
            return std::nullopt;
        }
        return poll(std::forward<decltype(on_message)>(on_message));
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message) {
        return terminate(std::forward<decltype(on_message)>(on_message), []() {});
    }

#ifdef BRIEFKASTEN_STALL_TRACE
    /// Dumps the queue state at most every BRIEFKASTEN_STALL_TRACE_SECONDS. Compare two dumps of a hanging
    /// run: what did not change is the stall.
    void stall_trace_tick() {
        if (stall_trace_interval_ <= 0.0) {
            return;
        }
        auto const now = std::chrono::steady_clock::now();
        if (std::chrono::duration<double>(now - stall_trace_last_).count() < stall_trace_interval_) {
            return;
        }
        stall_trace_last_ = now;
        auto const counts = message_counts();
        std::ostringstream out;
        out << "[bk-stall rank " << rank() << "] pending=" << pending_elements()
            << " buffered=" << global_buffer_size_ << " parked=" << parked_elements_
            << " parked_peers=" << parked_peers_.size() << " buffers=" << num_aggregation_buffers_
            << "/" << buffer_limit() << " free=" << free_aggregation_buffers_.size()
            << " parked_for_credit=" << num_parked_for_credit_
            << " parked_for_capacity=" << num_parked_for_capacity_
            << " redirect_buffer_stalls=" << num_redirect_buffer_stalls_
            << " redirect_overdraft=" << redirect_overdraft_
            << " buffer_stalls=" << num_buffer_stalls_ << " sends=" << counts.send
            << " recvs=" << counts.receive << " term_calls=" << queue_.num_terminate_calls()
            << " term_drains=" << queue_.num_termination_drains()
            << " term_rounds=" << num_termination_rounds() << " polls=" << num_polls()
            << " unproductive=" << num_unproductive_polls() << " deaf=" << num_deaf_probes()
            << " overflow_waits=" << num_overflow_capacity_waits()
            << " drain_waits=" << num_drain_capacity_waits()
            << " | acquires=" << num_buffer_acquires_ << " recycles=" << num_buffer_recycles_
            << " reclaims=" << num_buffer_reclaims_ << "\n    " << flow_.describe();
        for (auto const& entry : parked_) {
            std::size_t elements = 0;
            for (auto const& parked : entry.second) {
                elements += parked.buffer.size();
            }
            out << "\n    PARKED to " << entry.first << ": " << entry.second.size() << " buffers, " << elements
                << " elements";
        }
        for (auto const& entry : aggregation_buffers_) {
            if (!entry.second.empty()) {
                out << "\n    buffering for " << entry.first << ": " << entry.second.size() << " elements";
            }
        }
        out << "\n";
        std::fputs(out.str().c_str(), stderr);
    }

#endif  // BRIEFKASTEN_STALL_TRACE

    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message, std::invocable<> auto&& progress_hook) {
        BRIEFKASTEN_STALL_TRACE_TICK();
        // MessageQueue::terminate polls the underlying queue only, so grants and parked buffers are
        // progressed here
        auto drive = [&] {
            flow_.poll();
            send_parked();
            progress_hook();
        };
        auto before_next_message_counting_round_hook = [&] { drive(); };
        // Drain only right before counting, not on every termination attempt (most are aborted, and
        // draining forces out partially filled buffers). Buffered and parked payload is reported as
        // `pending`, so termination cannot fire while a proxy still holds data.
        auto prepare_and_count = [&] {
            flush_all_buffers_blocking(
                on_message, [&] { return termination_state() == TerminationState::active; }, drive);
            return internal::MessageCounter{.send = 0, .receive = 0, .pending = pending_elements()};
        };
        return queue_.terminate(
            split_handler(on_message),
            [&](std::size_t receipt, BufferContainer buffer) {
                reclaim_aggregation_buffer(receipt, std::move(buffer));
            },
            before_next_message_counting_round_hook, drive, prepare_and_count);
    }

    /// Underlying buffer counts plus this queue's buffered payload.
    [[nodiscard]] internal::MessageCounter message_counts() const {
        auto counts = queue_.message_counts();
        counts.pending += pending_elements();
        return counts;
    }

    /// Elements in aggregation buffers or parked, i.e. not yet handed to MPI.
    [[nodiscard]] std::size_t pending_elements() const {
        KASSERT(!aggregation_buffers_.empty() || global_buffer_size_ == 0,
                "buffer map is empty but " << global_buffer_size_
                                           << " elements are still counted as buffered");
        return global_buffer_size_ + parked_elements_;
    }

    /// Flush every aggregation buffer, blocking only while send slots are exhausted. Polls before each flush
    /// and stops early once \p should_stop returns true (i.e. new work arrived).
    void flush_all_buffers_blocking(MessageHandler<MessageType> auto&& on_message,
                                    std::predicate auto&& should_stop,
                                    std::invocable<> auto&& progress_hook) {
        // iterate over a snapshot of the destinations: polling may run a redirection handler that modifies
        // aggregation_buffers_. Buffers created meanwhile are left for the next drain.
        KASSERT(!draining_, "flush_all_buffers_blocking is not re-entrant");
        draining_ = true;
        drain_targets_.clear();
        drain_targets_.reserve(aggregation_buffers_.size());
        for (auto const& entry : aggregation_buffers_) {
            drain_targets_.push_back(entry.first);
        }
        auto finish = [&] { draining_ = false; };
        for (PEID target : drain_targets_) {
            poll(on_message);  // observe arrivals (may flip should_stop) and progress sends
            progress_hook();
            if (should_stop()) {
                finish();
                return;
            }
            // with flow control a flush never fails (it parks), so there is nothing to wait for
            while (!flow_.enabled() && !queue_.has_send_capacity()) {
                num_drain_capacity_waits_++;
                poll(on_message);  // only block when slots are exhausted; polling frees them as peers receive
                progress_hook();
                if (should_stop()) {
                    finish();
                    return;
                }
            }
            auto it = aggregation_buffers_.find(target);
            if (it == aggregation_buffers_.end()) {
                continue;  // a nested redirect flushed it while we were polling
            }
            // selective drain: skip buffers that grew since the last drain, they will fill up on their own
            if (selective_drain_) {
                auto& last_seen = last_drain_size_[it->first];
                auto current = it->second.size();
                if (current > last_seen) {
                    last_seen = current;
                    num_drain_skips_++;
                    continue;
                }
            }
            bool flushed = false;
            forced_flush_ = true;
            if (selective_drain_) {
                last_drain_size_.erase(it->first);
            }
            std::tie(std::ignore, flushed) = flush_buffer_impl(it, /*erase=*/true);
            forced_flush_ = false;
            KASSERT(flushed, "Flush must succeed once send capacity is ensured.");
        }
        finish();
    }

    /// Skip actively-filling buffers when draining for termination. Off by default.
    void selective_drain(bool enable) {
        selective_drain_ = enable;
        if (!enable) {
            last_drain_size_.clear();
        }
    }

    [[nodiscard]] bool selective_drain() const {
        return selective_drain_;
    }

    /// Buffers the selective drain declined to force out because they were still filling.
    [[nodiscard]] std::size_t num_drain_skips() const {
        return num_drain_skips_;
    }

    void flush_all_buffers_blocking(MessageHandler<MessageType> auto&& on_message, std::predicate auto&& should_stop) {
        flush_all_buffers_blocking(std::forward<decltype(on_message)>(on_message),
                                   std::forward<decltype(should_stop)>(should_stop), [] {});
    }

    /// Attempt termination only every \p skip_threshold-th call; otherwise poll and return false.
    [[nodiscard]] bool terminate_throttled(MessageHandler<MessageType> auto&& on_message,
                                           std::size_t skip_threshold = 1) {
        if (skip_threshold > 1 && (terminate_call_count_++ % skip_threshold) != 0) {
            poll(on_message);
            return false;
        }
        return terminate(std::forward<decltype(on_message)>(on_message));
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
        // snapshot the destinations: polling may modify aggregation_buffers_
        std::vector<PEID> targets;
        targets.reserve(aggregation_buffers_.size());
        for (auto const& entry : aggregation_buffers_) {
            targets.push_back(entry.first);
        }
        for (PEID target : targets) {
            auto current = aggregation_buffers_.find(target);
            if (current != aggregation_buffers_.end() && check_for_local_buffer_overflow(current->second, 0)) {
                resolve_overflow_blocking(target, on_message, [] {});
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
        return user_config_;
    }

    /// Set by IndirectionAdapter from its routing scheme: whether buffers from a peer may contain messages this
    /// rank has to forward. If unset (flat queue), nothing is ever redirected.
    void set_may_redirect(std::function<bool(PEID)> may_redirect) {
        may_redirect_ = std::move(may_redirect);
        may_redirect_cache_.clear();
    }

    /// Cached, so the std::function is called once per peer.
    [[nodiscard]] bool may_redirect(PEID peer) const {
        if (!may_redirect_) {
            return false;
        }
        auto it = may_redirect_cache_.find(peer);
        if (it != may_redirect_cache_.end()) {
            return it->second;
        }
        auto redirect = may_redirect_(peer);
        may_redirect_cache_.emplace(peer, redirect);
        return redirect;
    }

    /// Raise (or lower) the cap on concurrently held aggregation buffers. The cap only bounds lazy growth in
    /// acquire_buffer(), so adjusting it after construction is safe; already-allocated buffers are untouched.
    void max_num_aggregation_buffers(std::size_t new_max) {
        max_num_aggregation_buffers_ = new_max;
        effective_config_.max_num_aggregation_buffers = new_max;
    }

    [[nodiscard]] std::size_t max_num_aggregation_buffers() const {
        return max_num_aggregation_buffers_;
    }

    /// Adjust the underlying send backlog capacity at runtime (see Sender::set_send_backlog_capacity).
    void send_backlog_capacity(std::size_t new_capacity) {
        effective_config_.send_backlog_capacity = new_capacity;
        queue_.set_send_backlog_capacity(new_capacity);
    }

    [[nodiscard]] std::size_t send_backlog_capacity() const {
        return effective_config_.send_backlog_capacity.value();
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

    [[nodiscard]] std::size_t num_overflows() const {
        return num_overflows_;
    }

    [[nodiscard]] std::size_t num_elements_flushed() const {
        return num_elements_flushed_;
    }

    [[nodiscard]] std::size_t num_buffer_stalls() const {
        return num_buffer_stalls_;
    }

    /// Flushes forced by the termination drain rather than by a full buffer.
    [[nodiscard]] std::size_t num_forced_flushes() const {
        return num_forced_flushes_;
    }

    [[nodiscard]] std::size_t num_forced_flush_elements() const {
        return num_forced_flush_elements_;
    }

    [[nodiscard]] std::size_t num_termination_rounds() const {
        return queue_.num_termination_rounds();
    }

    /// See MessageQueue::num_terminate_calls / num_termination_drains.
    [[nodiscard]] std::size_t num_terminate_calls() const {
        return queue_.num_terminate_calls();
    }

    [[nodiscard]] std::size_t num_termination_drains() const {
        return queue_.num_termination_drains();
    }

    /// Spins waiting for a free send slot, in the termination drain plus in the post path.
    [[nodiscard]] std::size_t num_send_capacity_waits() const {
        return num_drain_capacity_waits_ + num_overflow_capacity_waits_;
    }

    /// Spins waiting for a free send slot in \ref flush_all_buffers_blocking (termination drain).
    [[nodiscard]] std::size_t num_drain_capacity_waits() const {
        return num_drain_capacity_waits_;
    }

    /// Spins waiting for a free send slot in \ref resolve_overflow_blocking (post path).
    [[nodiscard]] std::size_t num_overflow_capacity_waits() const {
        return num_overflow_capacity_waits_;
    }

    /// Buffers parked because no request slot was free.
    [[nodiscard]] std::size_t num_parked_for_capacity() const {
        return num_parked_for_capacity_;
    }

    /// Buffers parked because the receiver had not granted enough credit.
    [[nodiscard]] std::size_t num_parked_for_credit() const {
        return num_parked_for_credit_;
    }

    /// Elements currently parked.
    [[nodiscard]] std::size_t parked_elements() const {
        return parked_elements_;
    }

    /// Redirected elements currently held for forwarding.
    [[nodiscard]] std::size_t num_redirect_elements_buffered() const {
        return flow_.num_redirect_elements_buffered();
    }

    /// Times the proxy had to wait for a buffer; should be zero.
    [[nodiscard]] std::size_t num_redirect_buffer_stalls() const {
        return num_redirect_buffer_stalls_;
    }

    /// Upper bound on \ref redirect_overdraft, in buffers.
    [[nodiscard]] std::size_t redirect_pool_ceiling() const {
        auto const buffer_size = std::max<std::size_t>(queue_.reserved_receive_buffer_size(), 1);
        return (flow_.redirect_high_water() / buffer_size) + num_peers_ + 1;
    }

    /// Buffers the proxy allocated beyond the pool cap (the proxy never waits for a buffer).
    [[nodiscard]] std::size_t redirect_overdraft() const {
        return redirect_overdraft_;
    }

    [[nodiscard]] std::size_t num_grants_withheld() const {
        return flow_.num_grants_withheld();
    }

    [[nodiscard]] std::size_t num_grants_sent() const {
        return flow_.num_grants_sent();
    }

    [[nodiscard]] std::size_t num_grants_received() const {
        return flow_.num_grants_received();
    }

    /// Buffers larger than a whole window, sent anyway (see FlowController::has_credit).
    [[nodiscard]] std::size_t num_oversize_passes() const {
        return flow_.num_oversize_passes();
    }

    [[nodiscard]] bool flow_control_enabled() const {
        return flow_.enabled();
    }

    /// Credit window per peer, in elements.
    [[nodiscard]] std::size_t flow_control_num_window_elements() const {
        return flow_.num_window_elements();
    }

    [[nodiscard]] std::size_t num_polls() const {
        return queue_.num_polls();
    }

    [[nodiscard]] std::size_t num_unproductive_polls() const {
        return queue_.num_unproductive_polls();
    }

    /// Receive (re-)arms issued over the underlying queue's lifetime; not reset by \ref reset_stats.
    [[nodiscard]] std::size_t num_receive_arms() const {
        return queue_.num_receive_arms();
    }

    /// How deeply receive handling nested; see \ref internal::ReceiveNestingCounter.
    [[nodiscard]] std::size_t max_probe_depth() const {
        return queue_.max_probe_depth();
    }

    [[nodiscard]] std::size_t num_nested_probes() const {
        return queue_.num_nested_probes();
    }

    [[nodiscard]] std::size_t max_disarmed_slots() const {
        return queue_.max_disarmed_slots();
    }

    [[nodiscard]] std::size_t num_half_deaf_probes() const {
        return queue_.num_half_deaf_probes();
    }

    [[nodiscard]] std::size_t num_deaf_probes() const {
        return queue_.num_deaf_probes();
    }

    [[nodiscard]] std::size_t num_immediate_sends() const {
        return queue_.num_immediate_sends();
    }

    [[nodiscard]] std::size_t num_backlogged_sends() const {
        return queue_.num_backlogged_sends();
    }

    [[nodiscard]] std::size_t num_send_capacity_misses() const {
        return queue_.num_send_capacity_misses();
    }

    [[nodiscard]] std::size_t peak_send_backlog() const {
        return queue_.peak_send_backlog();
    }

    void reset_stats() {
        num_overflows_ = 0;
        num_elements_flushed_ = 0;
        num_buffer_stalls_ = 0;
        num_drain_capacity_waits_ = 0;
        num_overflow_capacity_waits_ = 0;
        num_forced_flushes_ = 0;
        num_forced_flush_elements_ = 0;
        num_drain_skips_ = 0;
        num_parked_for_credit_ = 0;
        num_parked_for_capacity_ = 0;
        num_redirect_buffer_stalls_ = 0;
        flow_.reset_counters();
        queue_.reset_counters();
    }

private:
    using BufferMap = std::unordered_map<PEID, BufferContainer>;
    using BufferList = std::vector<BufferContainer>;

    /// \c redirected: how many of its elements are redirected, no longer counted as buffered once sent.
    struct ParkedBuffer {
        BufferContainer buffer;
        std::size_t redirected = 0;
    };

    // Fan-out for the direct case is p: every rank is a potential destination.
    static Config apply_comm_size_defaults(MPI_Comm comm, Config config) {
        int size;
        MPI_Comm_size(comm, &size);
        return apply_fan_out_defaults(std::move(config), static_cast<std::size_t>(size));
    }

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

    [[nodiscard]] std::size_t buffer_limit() const {
        return max_num_aggregation_buffers_ + redirect_overdraft_;
    }

    void reserve_aggregation_buffers(std::size_t num_buffers) {
        auto buffer_size = queue_.reserved_receive_buffer_size();
        reserve_aggregation_buffers(num_buffers, buffer_size);
    }

    // NOLINTNEXTLINE(*-easily-swappable-parameters)
    void reserve_aggregation_buffers(std::size_t num_buffers, std::size_t buffer_size) {
        if (num_aggregation_buffers_ + num_buffers > buffer_limit()) {
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

    /// \return a free buffer, or nullopt if the caller must wait for one. The proxy never waits: it grows
    /// the pool beyond the cap instead, since redirected payload is already bounded by the flow control.
    auto acquire_buffer() -> std::optional<BufferContainer> {
        bool const for_redirect = redirecting_depth_ > 0;
        if (free_aggregation_buffers_.empty()) {
            if (for_redirect && num_aggregation_buffers_ >= max_num_aggregation_buffers_) {
                KASSERT(redirect_overdraft_ < redirect_pool_ceiling(),
                        "proxy overdrew the buffer pool by " << redirect_overdraft_
                            << " buffers, past the " << redirect_pool_ceiling()
                            << " that credits should have bounded it to");
                redirect_overdraft_++;
            }
            if (num_aggregation_buffers_ < buffer_limit()) {
                reserve_aggregation_buffers(1);
            } else {
                // Heuristic: at the cap with no free buffer -> flush one.
                // It won’t free capacity immediately, but once the send
                // completes the buffer will be recycled via reclaim_aggregation_buffer
                if (aggregation_buffers_.size() >= buffer_limit()) {
                    flush_largest_buffer();
                }
                num_buffer_stalls_++;
                num_redirect_buffer_stalls_ += for_redirect ? 1 : 0;
                return std::nullopt;
            }
        }
        KASSERT(!free_aggregation_buffers_.empty());
        num_buffer_acquires_++;
        auto buffer = std::move(free_aggregation_buffers_.back());
        free_aggregation_buffers_.pop_back();
        return buffer;
    };

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    ///
    /// Both customization points may poll, which may run a redirection handler that posts into this queue. So
    /// aggregation_buffers_ may change underneath, and `it` is looked up again after each such call.
    bool post_message_impl(InputMessageRange<MessageType> auto&& message,
                           PEID receiver,  // NOLINT(*-easily-swappable-parameters)
                           PEID envelope_sender,
                           PEID envelope_receiver,
                           int tag,
                           OverflowHandler<BufferMap> auto&& handle_overflow,
                           BufferProvider<BufferContainer> auto&& get_new_buffer) {
        num_posts_++;  // checked in split_handler
        auto it = aggregation_buffers_.find(receiver);
        if (it == aggregation_buffers_.end()) {
            auto buffer = get_new_buffer();
            // a redirection handler may have created the entry meanwhile
            it = aggregation_buffers_.find(receiver);
            if (it == aggregation_buffers_.end()) {
                std::tie(it, std::ignore) = aggregation_buffers_.emplace(receiver, std::move(buffer));
            } else {
                recycle_buffer(std::move(buffer));
            }
        }

        auto envelope =
            MessageEnvelope{std::forward<decltype(message)>(message), envelope_sender, envelope_receiver, tag};
        bool overflow = false;
        // loop: while we poll, a redirection handler may refill this destination's buffer, so re-check
        while (true) {
            size_t estimated_new_buffer_size = 0;
            if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
                estimated_new_buffer_size =
                    merge.estimate_new_buffer_size(it->second, receiver, queue_.rank(), envelope);
            } else {
                estimated_new_buffer_size = it->second.size() + envelope.message.size();
            }
            if (!check_for_buffer_overflow(it->second, estimated_new_buffer_size - it->second.size())) {
                break;
            }
            overflow = true;
            num_overflows_++;
            handle_overflow(it);             // may poll, `it` is invalid afterwards
            auto buffer = get_new_buffer();  // may poll
            it = aggregation_buffers_.find(receiver);
            if (it == aggregation_buffers_.end()) {
                std::tie(it, std::ignore) = aggregation_buffers_.emplace(receiver, std::move(buffer));
            } else if (it->second.empty()) {
                // may still own a pool buffer, so recycle it instead of overwriting it
                recycle_buffer(std::move(it->second));
                it->second = std::move(buffer);
            } else {
                // refilled by a redirection handler, keep its payload
                recycle_buffer(std::move(buffer));
            }
        }
        auto& buffer = it->second;
        auto old_buffer_size = buffer.size();
        merge(buffer, receiver, queue_.rank(), std::move(envelope));
        auto new_buffer_size = buffer.size();
        auto const merged = new_buffer_size - old_buffer_size;
        global_buffer_size_ += merged;
        if (redirecting_depth_ > 0 && merged > 0) {
            redirected_in_buffer_[receiver] += merged;
            flow_.hold_redirected(merged);
        }
        return overflow;
    }

    /// @return an iterator to the next buffer (and true), or the input iterator (and false) if flushing failed
    auto flush_buffer_impl(BufferMap::iterator buffer_it, bool erase = true)
        -> std::pair<typename BufferMap::iterator, bool> {
        KASSERT(buffer_it != aggregation_buffers_.end(), "Trying to flush non-existing buffer.");
        auto& [receiver, buffer] = *buffer_it;
        if (buffer.empty()) {
            if (erase) {
                // back to the pool: an empty entry may still own a buffer
                BufferContainer container = std::move(buffer_it->second);
                auto next = aggregation_buffers_.erase(buffer_it);
                recycle_buffer(std::move(container));
                return {next, true};
            }
            return {++buffer_it, true};
        }
        auto pre_cleanup_buffer_size = buffer.size();
        pre_send_cleanup(buffer, receiver);
        // we don't send if the cleanup has emptied the buffer
        if (buffer.empty()) {
            global_buffer_size_ -= pre_cleanup_buffer_size;
            flow_.release_redirected(take_redirected(receiver));
            if (erase) {
                BufferContainer container = std::move(buffer_it->second);
                auto next = aggregation_buffers_.erase(buffer_it);
                free_aggregation_buffers_.emplace_back(std::move(container));
                return {next, true};
            }
            return {++buffer_it, true};
        }
        auto const elements = buffer_it->second.size();
        if (forced_flush_) {
            num_forced_flushes_++;
            num_forced_flush_elements_ += elements;
        }
        // With flow control a flush never fails: without credit or a free send slot, the buffer is parked
        // and send_parked() sends it later.
        if (flow_.enabled()) {
            bool const no_credit = !flow_.has_credit(receiver, elements);
            if (no_credit || !queue_.has_send_capacity()) {
                if (no_credit) {
                    num_parked_for_credit_++;
                } else {
                    num_parked_for_capacity_++;
                }
                park_buffer(receiver, std::move(buffer_it->second));
                global_buffer_size_ -= pre_cleanup_buffer_size;
                if (erase) {
                    return {aggregation_buffers_.erase(buffer_it), true};
                }
                return {++buffer_it, true};
            }
        } else if (!queue_.has_send_capacity()) {
            return {buffer_it, false};
        }
        num_elements_flushed_ += elements;
        auto const redirected = take_redirected(receiver);
        auto receipt = queue_.post_message(std::move(buffer_it->second), receiver);
        KASSERT(receipt.has_value(),
                "We checked before that there is capacity, so posting the message should not fail.");
        flow_.track_send(receiver, elements);
        if (redirected > 0) {
            redirected_by_receipt_[*receipt] = redirected;
        }
        global_buffer_size_ -= pre_cleanup_buffer_size;
        if (erase) {
            return {aggregation_buffers_.erase(buffer_it), true};
        }
        return {++buffer_it, true};
    }

    void park_buffer(PEID receiver, BufferContainer&& buffer) {
        auto const elements = buffer.size();
        auto& queue_for_peer = parked_[receiver];
        if (queue_for_peer.empty()) {
            parked_peers_.push_back(receiver);
        }
        queue_for_peer.push_back(ParkedBuffer{.buffer = std::move(buffer), .redirected = take_redirected(receiver)});
        parked_elements_ += elements;
    }

    /// Send parked buffers for which there is now credit and a free send slot.
    void send_parked() {
        if (parked_peers_.empty()) {
            return;
        }
        std::size_t kept = 0;
        for (std::size_t i = 0; i < parked_peers_.size(); ++i) {
            PEID receiver = parked_peers_[i];
            auto it = parked_.find(receiver);
            if (it == parked_.end()) {
                continue;
            }
            auto& parked = it->second;
            while (!parked.empty()) {
                auto const elements = parked.front().buffer.size();
                if (!flow_.has_credit(receiver, elements) || !queue_.has_send_capacity()) {
                    break;
                }
                auto const redirected = parked.front().redirected;
                auto receipt = queue_.post_message(std::move(parked.front().buffer), receiver);
                KASSERT(receipt.has_value(), "capacity was checked, so posting must succeed");
                flow_.track_send(receiver, elements);
                if (redirected > 0) {
                    redirected_by_receipt_[*receipt] = redirected;
                }
                num_elements_flushed_ += elements;
                parked_elements_ -= elements;
                parked.pop_front();
            }
            if (parked.empty()) {
                parked_.erase(it);
            } else {
                parked_peers_[kept++] = receiver;
            }
        }
        parked_peers_.resize(kept);
    }

    /// Redirected elements in \p receiver's current buffer; resets the count, since the buffer is being sent or parked.
    std::size_t take_redirected(PEID receiver) {
        auto it = redirected_in_buffer_.find(receiver);
        if (it == redirected_in_buffer_.end()) {
            return 0;
        }
        auto const redirected = it->second;
        redirected_in_buffer_.erase(it);
        return redirected;
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

    /// Splits an arriving buffer into messages and calls \p on_message on each.
    auto split_handler(MessageHandler<MessageType> auto&& on_message) {
        return [&](Envelope<BufferType> auto buffer) {
            auto const source = buffer.sender;
            auto const posts_before = num_posts_;
            auto const elements = buffer.message.size();
            // posts made while redirecting_depth_ > 0 are forwards and are counted as buffered redirect elements
            bool const redirects = may_redirect(source);
            flow_.set_proxied(source, redirects);
            redirecting_depth_ += redirects ? 1 : 0;
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
            redirecting_depth_ -= redirects ? 1 : 0;
            flow_.track_receive(source, elements);
            // Handlers must not send, except for the forwarding by the proxy. Otherwise flow control can deadlock.
            KASSERT(may_redirect(source) || num_posts_ == posts_before,
                    "posted " << (num_posts_ - posts_before) << " message(s) while handling a buffer from rank "
                              << source
                              << ", which is not a may_redirect peer. Either the indirection scheme is wrong, or the "
                                 "application sends from inside a message handler.");
        };
    }

    /// Return a buffer that was never sent to the pool.
    void recycle_buffer(BufferContainer&& buffer) {
        num_buffer_recycles_++;
        buffer.resize(0);  // this does not reduce the capacity
        free_aggregation_buffers_.emplace_back(std::move(buffer));
    }

    /// Called when a send completed; releases its redirected elements.
    auto reclaim_aggregation_buffer(std::size_t receipt, BufferContainer&& buffer) {
        num_buffer_reclaims_++;
        auto it = redirected_by_receipt_.find(receipt);
        if (it != redirected_by_receipt_.end()) {
            flow_.release_redirected(it->second);
            redirected_by_receipt_.erase(it);
        }
        recycle_buffer(std::move(buffer));
    }

    /// @return returns false iff resolve failed
    ///
    /// \p current_receiver is the destination whose buffer overflowed, or nullopt for a global overflow.
    bool resolve_overflow(std::optional<PEID> current_receiver) {
        auto current_buffer =
            current_receiver ? aggregation_buffers_.find(*current_receiver) : aggregation_buffers_.end();
        switch (flush_strategy_) {
            case FlushStrategy::local: {
                if (current_buffer == aggregation_buffers_.end()) {
                    // already flushed by a redirection handler while we polled
                    return true;
                }
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

    void resolve_overflow_blocking(std::optional<PEID> current_receiver,
                                   MessageHandler<MessageType> auto&& on_message,
                                   std::invocable<> auto&& progress_hook) {
        if (flow_.enabled()) {
            // the flush below parks instead of failing, so no need to wait. Poll once to make progress,
            // but not from inside a redirection handler, which would nest receive handling.
            if (redirecting_depth_ == 0) {
                poll(std::forward<decltype(on_message)>(on_message));
                progress_hook();
            }
        } else {
            // block only while send slots are exhausted; polling frees them as peers receive
            while (!queue_.has_send_capacity()) {
                num_overflow_capacity_waits_++;
                poll(std::forward<decltype(on_message)>(on_message));
                progress_hook();
            }
        }
        // capacity is ensured, so the flush must succeed
        bool success = resolve_overflow(current_receiver);
        if (success) {
            return;
        }
        throw std::runtime_error("Failed to resolve overflow in post_message_blocking. This should not happen.");
    }
    void resolve_overflow_blocking(MessageHandler<MessageType> auto&& on_message,
                                   std::invocable<> auto&& progress_hook) {
        resolve_overflow_blocking(std::nullopt, std::forward<decltype(on_message)>(on_message),
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

    Config user_config_;
    Config effective_config_;
    MessageQueue<BufferType, BufferContainer, ReceiveBufferContainer, Receiver> queue_;
    BufferMap aggregation_buffers_;
    BufferList free_aggregation_buffers_;
    size_t local_threshold_bytes_;
    size_t global_threshold_bytes_;
    std::size_t max_num_aggregation_buffers_;

    std::size_t num_aggregation_buffers_ = 0;
    std::size_t num_overflows_ = 0;
    std::size_t num_elements_flushed_ = 0;
    std::size_t num_buffer_stalls_ = 0;
    std::size_t num_drain_capacity_waits_ = 0;
    std::size_t num_overflow_capacity_waits_ = 0;
    std::size_t terminate_call_count_ = 0;
    std::size_t num_forced_flushes_ = 0;
    std::size_t num_forced_flush_elements_ = 0;
    std::size_t num_drain_skips_ = 0;
    bool selective_drain_ = false;
    std::unordered_map<PEID, std::size_t> last_drain_size_;  ///< buffer size at the previous drain
    bool forced_flush_ = false;                              ///< set while the termination drain flushes
    std::vector<PEID> drain_targets_;                        ///< scratch for flush_all_buffers_blocking
    bool draining_ = false;
    std::size_t num_posts_ = 0;  ///< calls to post_message_impl, checked in split_handler
    std::function<bool(PEID)> may_redirect_;
    mutable std::unordered_map<PEID, bool> may_redirect_cache_;

    internal::FlowController flow_;
    std::unordered_map<PEID, std::deque<ParkedBuffer>> parked_;  ///< per destination, waiting for credit
    std::vector<PEID> parked_peers_;                             ///< destinations with parked buffers
    std::size_t parked_elements_ = 0;
    std::unordered_map<PEID, std::size_t> redirected_in_buffer_;         ///< redirected elements per filling buffer
    std::unordered_map<std::size_t, std::size_t> redirected_by_receipt_;  ///< redirected elements per in-flight send
    std::size_t redirecting_depth_ = 0;  ///< nesting depth of handlers for buffers from may_redirect peers
    std::size_t num_parked_for_credit_ = 0;
    std::size_t num_parked_for_capacity_ = 0;
    std::size_t num_redirect_buffer_stalls_ = 0;
    std::size_t redirect_overdraft_ = 0;
    std::size_t num_peers_ = 0;
    std::size_t poll_throttle_count_ = 0;
    // only read by the stall tracer
    std::size_t num_buffer_acquires_ = 0;
    std::size_t num_buffer_recycles_ = 0;
    std::size_t num_buffer_reclaims_ = 0;
#ifdef BRIEFKASTEN_STALL_TRACE
    double stall_trace_interval_ = 0.0;
    std::chrono::steady_clock::time_point stall_trace_last_{};
#endif

    Merger merge;
    Splitter split;
    BufferCleaner pre_send_cleanup;
    size_t global_buffer_size_ = 0;

    FlushStrategy flush_strategy_;
};
}  // namespace briefkasten
