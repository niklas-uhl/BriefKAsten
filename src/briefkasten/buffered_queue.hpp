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
#include "./detail/link_class.hpp"
#include "./detail/queue.hpp"

namespace briefkasten {

static constexpr std::size_t DEFAULT_NUM_REQUEST_SLOTS = 8;
static constexpr std::size_t DEFAULT_BUFFER_THRESHOLD = 32ULL * 1024;

enum class FlushStrategy : std::uint8_t { local, global, random, largest };

struct Config {
    size_t num_request_slots = DEFAULT_NUM_REQUEST_SLOTS;
    std::optional<std::size_t> max_num_aggregation_buffers = std::nullopt;
    FlushStrategy flush_strategy = FlushStrategy::local;
    size_t global_threshold_bytes = std::numeric_limits<size_t>::max();
    std::size_t local_threshold_bytes = DEFAULT_BUFFER_THRESHOLD;
    std::optional<std::size_t> send_backlog_capacity = std::nullopt;
};

/// Apply double-buffering defaults to \p config for a queue with at most \p fan_out distinct
/// destinations, leaving any field that was set explicitly (i.e. differs from Config{}) untouched.
///
/// Sizes for double buffering so a destination never stalls on a premature flush:
///   send_backlog_capacity       = fan_out   (absorbs up to fan_out concurrent flushes without blocking)
///   max_num_aggregation_buffers = send_backlog_capacity + fan_out + num_request_slots
///                               = backlog + fan_out (filling) + num_request_slots (in flight)
///
/// The buffer pool is derived from the *backlog*, not from fan_out a second time, because a backlogged
/// send owns its aggregation buffer until it is actually posted. Sizing the pool at 2*fan_out while the
/// caller raised send_backlog_capacity leaves the backlog unreachable: the pool runs dry first, so every
/// wait for send capacity simply becomes a buffer stall at an unchanged total in-flight capacity, and the
/// knob looks inert. Observed directly in relay-backpressure_26_09_15 (rmat n18 p76: 375,806 capacity
/// waits -> 375,786 buffer stalls, runtime unchanged to the millisecond).
///
/// At the default (send_backlog_capacity == fan_out) this is exactly the old 2*fan_out + num_request_slots,
/// so nothing changes unless the caller sets the backlog explicitly.
///
/// Buffers are allocated lazily, so sparse workloads pay only for their active destinations.
/// For large fan_out, startup overhead (MPI connection setup, NIC resources) grows with the number
/// of distinct partners — buffer sizing cannot address that. Use IndirectionAdapter to reduce live
/// partners to O(sqrt(p)) when startup overhead dominates.
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
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)),
          flush_strategy_(effective_config_.flush_strategy) {
        reserve_aggregation_buffers(effective_config_.num_request_slots);
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
                // Keyed by `receiver`, not by the iterator we were handed: resolve_overflow_blocking polls,
                // and under a single-queue IndirectionAdapter a poll can rehash or erase aggregation_buffers_
                // underneath us. See post_message_impl's re-entrancy note.
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
    ///
    /// ONE COUNTING ROUND OVER ONE QUEUE. This used to have a four-argument sibling taking
    /// `additional_counts` and `extra_round_prepare`, which existed solely so IndirectionAdapter could fold
    /// a second hop's send/receive counts into this hop's allreduce -- two sequential per-hop terminations
    /// deadlock, because a locally reactivated rank leaves the first collective and enters the second while
    /// its peers are still in the first. The two-hop collapse (see indirection.hpp) removed the sibling
    /// queue, and with it the fold and the whole class of bug where a message merged into the sibling's
    /// buffer counted as received but not as sent.
    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message, std::invocable<> auto&& progress_hook) {
        auto before_next_message_counting_round_hook = [&] {
            flush_all_buffers_blocking(
                on_message, [&] { return termination_state() == TerminationState::active; }, progress_hook);
        };
        // Our own buffered payload -- including anything a relay received and merged but has not yet
        // forwarded -- joins the counting round. send/receive are counted per PACKET, so without this term
        // a relayed message sitting in a proxy's buffer is invisible: the packet that carried it was sent
        // once and received once, the counts balance, and termination fires with the data undelivered.
        auto prepare_and_count = [&] {
            return internal::MessageCounter{.send = 0, .receive = 0, .pending = pending_elements()};
        };
        return queue_.terminate(
            split_handler(on_message),
            [&](std::size_t receipt, BufferContainer buffer) {
                reclaim_aggregation_buffer(receipt, std::move(buffer));
            },
            before_next_message_counting_round_hook, progress_hook, prepare_and_count);
    }

    /// Underlying packet counts PLUS this queue's own outstanding buffer contents. A sibling queue
    /// folded into a joint termination round must be reported through this, not through the raw
    /// queue's counts, or its buffered payload stays invisible to the decision.
    [[nodiscard]] internal::MessageCounter message_counts() const {
        auto counts = queue_.message_counts();
        counts.pending += pending_elements();
        return counts;
    }

    /// Payload currently held in aggregation buffers, in buffer elements. Only its zero-ness is
    /// meaningful to termination. Already correct in the presence of a BufferCleaner: flush
    /// subtracts the PRE-cleanup size, so discarded payload is accounted for.
    [[nodiscard]] std::size_t pending_elements() const {
        return global_buffer_size_;
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
    ///
    /// \p progress_hook mirrors the one on \ref post_message_blocking and exists for the same reason: this loop can
    /// spin arbitrarily long on our own send capacity, and a sibling queue whose receiver is never driven during that
    /// spin cannot complete the very sends we are waiting on. This was the one blocking loop of four that took no
    /// hook, which is what made the invariant "every blocking loop drives the second hop" fail at termination time.
    /// See notes/takeover_relay_backpressure.md.
    void flush_all_buffers_blocking(MessageHandler<MessageType> auto&& on_message,
                                    std::predicate auto&& should_stop,
                                    std::invocable<> auto&& progress_hook) {
        // Iterate over a SNAPSHOT of the destinations, not over live iterators. Every poll() below can run a
        // relay handler that posts back into this queue (single-queue IndirectionAdapter), which rehashes
        // aggregation_buffers_ and may erase entries; an iterator held across a poll is a dangling read.
        // Destinations that appear DURING the drain are deliberately left for the next one: they hold
        // freshly relayed payload, MessageCounter::pending refuses termination while any payload is
        // buffered, and chasing them here would let a busy relay keep this loop running indefinitely.
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
            while (!queue_.has_send_capacity()) {
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
                continue;  // a nested relay post flushed it while we were polling
            }
            // Selective drain: skip a buffer that GREW since the last drain. Such a buffer is
            // actively filling and will reach its threshold on its own, so forcing it out now only
            // fragments the traffic -- the measured cost is 64% of relay packets at 5% fill at
            // p=608. A buffer that has stopped growing is the tail and must be forced, or nothing
            // ever empties it.
            //
            // Safe only because termination no longer depends on this loop emptying anything: the
            // `pending` term in the counting round refuses while any payload is buffered, so
            // skipping can delay termination but cannot let it fire with data undelivered. Before
            // that term existed this would have been a silent-data-loss bug.
            //
            // Progress: a buffer that keeps growing hits the local threshold and flushes itself; a
            // buffer that stops growing is stale by the next drain. So every buffer empties, and an
            // unseen buffer costs at most one extra drain (it is recorded as grown on first sight).
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
            // Everything this loop flushes is FORCED: it goes out at whatever fill it happens to have,
            // because termination needs the buffer empty, not full. Attributed so the aggregation cost of
            // the termination protocol is measurable rather than inferred from (sends - overflows).
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

    /// Skip actively-filling buffers when draining for termination; see flush_all_buffers_blocking.
    /// Requires the `pending` term in the termination round (MessageCounter::pending) -- without it
    /// this trades aggregation for silent data loss. Off by default.
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

    /// \overload for callers with no sibling queue to drive. Not a defaulted parameter: a default argument
    /// cannot deduce an abbreviated-template (`auto&&`) parameter.
    void flush_all_buffers_blocking(MessageHandler<MessageType> auto&& on_message, std::predicate auto&& should_stop) {
        flush_all_buffers_blocking(std::forward<decltype(on_message)>(on_message),
                                   std::forward<decltype(should_stop)>(should_stop), [] {});
    }

    /// Attempt termination only every \p skip_threshold-th call; otherwise poll and report "not done".
    ///
    /// Mirrors \ref poll_throttled, and for the same reason: the caller's loop is
    /// `do { while (work) ...; } while (!terminate());`, so terminate() is invoked every time the local
    /// work queue happens to empty -- which under an async traversal is constantly. Measured on rmat n18
    /// p128: 8,379 calls per rank per iteration against 3 that reached an allreduce. The other 8,376 ran
    /// the full protocol (own-buffer drain, outstanding-send wait, and under IndirectionAdapter a sibling
    /// drain) only to abort on an arriving message.
    ///
    /// The skipped path MUST still poll. A skip that merely returns false livelocks the caller: its work
    /// queue is empty, nothing else polls, so no message can ever arrive to refill it.
    ///
    /// Safe by construction in the direction that matters -- returning false early can only DELAY
    /// termination, never trigger it prematurely -- so the failure mode of a bad threshold is a slower
    /// run, not silent data loss.
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
        // Snapshot the destinations: resolve_overflow_blocking polls, which under indirection can rehash
        // this map from a relay handler. Same reasoning as flush_all_buffers_blocking.
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

    /// Install the link classifier for this queue's peers; see LinkClass.
    ///
    /// Set by IndirectionAdapter from its routing scheme. Left unset the queue classifies every peer as
    /// \ref LinkClass::to_destination, which is correct for a flat queue: nothing is ever relayed.
    ///
    /// Looked up at most once per peer and then cached, so the std::function indirection is paid O(peers)
    /// times per phase, not per message.
    void link_classifier(std::function<LinkClass(PEID)> classifier) {
        link_classifier_ = std::move(classifier);
        link_class_cache_.clear();
    }

    /// The class of the link to \p peer, memoised.
    [[nodiscard]] LinkClass link_class(PEID peer) const {
        if (!link_classifier_) {
            return LinkClass::to_destination;
        }
        auto it = link_class_cache_.find(peer);
        if (it != link_class_cache_.end()) {
            return it->second;
        }
        auto cls = link_classifier_(peer);
        link_class_cache_.emplace(peer, cls);
        return cls;
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

    /// Sends issued by \ref flush_all_buffers_blocking, i.e. by the termination protocol rather than by a
    /// buffer reaching its threshold. These go out at whatever fill they happen to have, so they are the
    /// aggregation tax of terminating. Compare \ref num_forced_flush_elements against these to get their
    /// average fill, and against \ref num_elements_flushed for their share of the traffic.
    ///
    /// Motivation: on rmat the *relay* hop packs its packets to 64-77% of threshold while the originating
    /// hop packs to 99%, and the excess sends on the busiest proxies are not overflows. This counter tells
    /// us directly whether the termination drain is where they come from. See
    /// notes/takeover_relay_backpressure.md.
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

    /// Iterations spent spinning because the sender had neither a free request slot nor backlog room,
    /// summed over both blocking sites. Complements \ref num_buffer_stalls, which only covers exhaustion
    /// of the *aggregation* buffer pool and stays at zero when the request pool is the bottleneck.
    ///
    /// Always read this together with its two components below: they sit on opposite sides of the
    /// termination boundary, and the total cannot distinguish them. Until 2026-09-15 only the drain site
    /// was counted, which made every wait look like a termination artifact.
    [[nodiscard]] std::size_t num_send_capacity_waits() const {
        return num_drain_capacity_waits_ + num_overflow_capacity_waits_;
    }

    /// Waits inside \ref flush_all_buffers_blocking, i.e. the *termination* drain: reached only from
    /// \ref terminate.
    [[nodiscard]] std::size_t num_drain_capacity_waits() const {
        return num_drain_capacity_waits_;
    }

    /// Waits inside \ref resolve_overflow_blocking, i.e. the *steady-state* post path: an aggregation
    /// buffer filled up and the flush that must precede the merge could not get send capacity.
    ///
    /// Under a single-queue IndirectionAdapter this is where the relay's blocking shows up: the relay
    /// handler reaches it through post_message_blocking(direct_send=true). It no longer separates the relay
    /// from the application the way the two-hop split did (both now report into one counter); what the
    /// stall investigation needed that split for is instead answered by the flow controller's own
    /// counters. See notes/takeover_briefkasten_tokens.md.
    [[nodiscard]] std::size_t num_overflow_capacity_waits() const {
        return num_overflow_capacity_waits_;
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

    /// How deeply receive handling nested and how many receive slots were disarmed meanwhile; see
    /// \ref internal::ReceiveNestingCounter. On an IndirectionAdapter the FIRST hop's values are the relay: its
    /// handler posts blocking into the second hop, which polls the first hop again from inside the handler.
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
        queue_.reset_counters();
    }

private:
    using BufferMap = std::unordered_map<PEID, BufferContainer>;
    using BufferList = std::vector<BufferContainer>;

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
                num_buffer_stalls_++;
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
    ///
    /// RE-ENTRANCY. Both customization points can poll, and under a single-queue IndirectionAdapter a poll
    /// runs the relay handler, which posts back into THIS queue: `aggregation_buffers_` may rehash (killing
    /// iterators) and the entry for `receiver` may even be erased outright (get_new_buffer's
    /// flush_largest_buffer path erases an empty buffer). So `it` is re-established by a fresh lookup after
    /// every such call rather than reused. Before the two-hop collapse the relay posted into a *different*
    /// queue object and none of this could happen; see indirection.hpp.
    bool post_message_impl(InputMessageRange<MessageType> auto&& message,
                           PEID receiver,  // NOLINT(*-easily-swappable-parameters)
                           PEID envelope_sender,
                           PEID envelope_receiver,
                           int tag,
                           OverflowHandler<BufferMap> auto&& handle_overflow,
                           BufferProvider<BufferContainer> auto&& get_new_buffer) {
        num_posts_++;  // observed by split_handler's acyclicity assertion
        auto it = aggregation_buffers_.find(receiver);
        if (it == aggregation_buffers_.end()) {
            auto buffer = get_new_buffer();
            std::tie(it, std::ignore) = aggregation_buffers_.insert_or_assign(receiver, std::move(buffer));
        }

        auto envelope =
            MessageEnvelope{std::forward<decltype(message)>(message), envelope_sender, envelope_receiver, tag};
        size_t estimated_new_buffer_size = 0;
        if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
            estimated_new_buffer_size = merge.estimate_new_buffer_size(it->second, receiver, queue_.rank(), envelope);
        } else {
            estimated_new_buffer_size = it->second.size() + envelope.message.size();
        }
        bool overflow = false;
        if (check_for_buffer_overflow(it->second, estimated_new_buffer_size - it->second.size())) {
            overflow = true;
            num_overflows_++;
            handle_overflow(it);             // customization point; may poll -> `it` is dead after this
            auto buffer = get_new_buffer();  // may poll too, for the same reason
            // insert_or_assign, not emplace: the local flush strategy leaves the entry in place holding a
            // moved-from shell, while a nested flush may have erased it. Both cases end up here.
            std::tie(it, std::ignore) = aggregation_buffers_.insert_or_assign(receiver, std::move(buffer));
        }
        // Read immediately before the merge, so that whatever a nested poll did to OTHER buffers (and to
        // global_buffer_size_) is already accounted for and this delta stays correct.
        auto& buffer = it->second;
        auto old_buffer_size = buffer.size();
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
        num_elements_flushed_ += buffer_it->second.size();
        if (forced_flush_) {
            num_forced_flushes_++;
            num_forced_flush_elements_ += buffer_it->second.size();
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

    /// Wraps the per-message handler around one arriving PACKET, and is where the receive side sees both
    /// the immediate MPI source (\c buffer.sender -- not \c env.sender, which under indirection is the
    /// original origin) and the packet's element count. That makes it the natural place to check the
    /// property the whole credit design rests on.
    auto split_handler(MessageHandler<MessageType> auto&& on_message) {
        return [&](Envelope<BufferType> auto buffer) {
            auto const source = buffer.sender;
            auto const posts_before = num_posts_;
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
            // ACYCLICITY. A to_destination link is terminal: handling what arrives over it must not post
            // anything, or the chain "different-column send -> same-column send -> delivery" is not a
            // chain and the deadlock argument in link_class.hpp is void. Checking it here rather than
            // asserting it in prose catches both a mis-stated scheme (a classifier that calls a relay link
            // terminal) and an application that sends from inside a message handler -- the precondition in
            // section 4.5 of notes/takeover_briefkasten_tokens.md, which is otherwise silent until it
            // deadlocks at scale. One integer compare per packet; compiled out at assertion level 0.
            KASSERT(link_class(source) == LinkClass::to_proxy || num_posts_ == posts_before,
                    "posted " << (num_posts_ - posts_before)
                              << " message(s) while handling a packet from rank " << source
                              << ", whose link is classified terminal (LinkClass::to_destination). Either the "
                                 "indirection scheme mis-classifies that link, or the application sends from "
                                 "inside a message handler.");
        };
    }

    auto reclaim_aggregation_buffer(std::size_t /*receipt*/, BufferContainer&& buffer) {
        buffer.resize(0);  // this does not reduce the capacity
        free_aggregation_buffers_.emplace_back(std::move(buffer));
    }

    /// @return returns false iff resolve failed
    ///
    /// \p current_receiver is the destination whose buffer overflowed, or nullopt when the overflow was
    /// global (no single buffer is "current"). Looked up here rather than passed as an iterator because
    /// every blocking caller polls first, and a poll can invalidate iterators (see post_message_impl).
    bool resolve_overflow(std::optional<PEID> current_receiver) {
        auto current_buffer =
            current_receiver ? aggregation_buffers_.find(*current_receiver) : aggregation_buffers_.end();
        switch (flush_strategy_) {
            case FlushStrategy::local: {
                if (current_buffer == aggregation_buffers_.end()) {
                    // A nested relay post already flushed (and erased) it while we waited for capacity.
                    // The buffer we were asked to make room in no longer exists, so there is nothing to do.
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
        // Block only while send slots are actually exhausted; polling frees them as peers receive.
        // Gate on send *capacity* (what the flush needs), not on a send *completion* event: if this queue
        // has no outstanding send, no completion will ever fire, so waiting for one deadlocks even though a
        // slot is already free and the flush could proceed immediately. This also matters because a receiver
        // that stops draining while blocked (e.g. ProbeReceiver, once its recursion guard has consumed every
        // receive slot) removes the incoming traffic whose receipt is what lets remote sends — and thus our
        // awaited local completion — make progress; PersistentReceiver masks this by never refusing to receive.
        // Mirrors flush_all_buffers_blocking.
        while (!queue_.has_send_capacity()) {
            num_overflow_capacity_waits_++;
            poll(std::forward<decltype(on_message)>(on_message));
            progress_hook();
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
    /// Buffer size seen at the previous drain, per destination. An unseen destination reads 0, so a
    /// non-empty buffer counts as "grew" on first sight and is skipped once.
    std::unordered_map<PEID, std::size_t> last_drain_size_;
    // Set only around flush_all_buffers_blocking's own flush call. Still safe as a plain flag rather than
    // a counter after the two-hop collapse, but for a narrower reason than before: a relay handler DOES
    // now post back into this queue, but only from the poll() at the top of the drain loop, which is
    // outside the window this flag is set in. flush_buffer_impl itself never polls, so nothing can
    // re-enter between the set and the clear. (The old reason -- that the handler posted into a different
    // queue object -- no longer holds; see indirection.hpp.)
    bool forced_flush_ = false;
    /// Reused scratch for flush_all_buffers_blocking's destination snapshot, so a drain that runs
    /// thousands of times per iteration does not allocate. Guarded by draining_, which asserts the loop
    /// is not re-entered (nothing calls terminate from inside a message handler).
    std::vector<PEID> drain_targets_;
    bool draining_ = false;
    /// Calls to post_message_impl. Only ever read as a difference across one packet's handling, to check
    /// that a terminal link's handler posts nothing; see split_handler.
    std::size_t num_posts_ = 0;
    std::function<LinkClass(PEID)> link_classifier_;
    mutable std::unordered_map<PEID, LinkClass> link_class_cache_;

    Merger merge;
    Splitter split;
    BufferCleaner pre_send_cleanup;
    size_t global_buffer_size_ = 0;

    FlushStrategy flush_strategy_;
};
}  // namespace briefkasten
