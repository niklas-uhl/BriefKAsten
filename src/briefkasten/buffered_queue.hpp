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

/// What terminate() flushes on entry, i.e. whenever the caller has run out of local work, before it checks
/// for arrivals. Independent of the termination drain, which only runs on attempts that reach a counting round.
enum class IdleFlush : std::uint8_t {
    off,    ///< leave partially filled buffers to the threshold and the termination drain (best packet fill)
    all,    ///< every non-empty buffer, as in v0.2.1 (lowest latency, smallest packets)
    stale,  ///< only buffers that did not grow since the previous terminate() call: a still-filling buffer
            ///< waits for its threshold, a tail goes out one idle attempt later
};

/// EXPERIMENT SWITCH: the order in which the termination drain flushes the aggregation buffers (it stops as soon as
/// new work arrives, so the order decides which buffers wait). map: the buffer map's iteration order, i.e. for
/// libstdc++'s std::unordered_map the most recently inserted destination first. rank: ascending destination rank.
/// rotating: ascending rank, starting at the destination where the previous drain stopped.
enum class DrainOrder : std::uint8_t { map, rank, rotating };

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
    /// IdleFlush::all for flat and IndirectionAdapter alike: with round robin it was the best async setting for
    /// ruler chasing on SuperMUC-NG up to 3072 cores (kascade, 2026-11-05); for the grid IdleFlush::stale tied.
    /// IdleFlush::off gives the fullest relay packets but leaves latency-bound workloads waiting for the drain.
    IdleFlush idle_flush = IdleFlush::all;
    /// Resume each idle flush at the destination where the previous one ran out of send slots, instead of at
    /// the start of the buffer map. Without it the same destinations go first every time and the rest only
    /// leave through the threshold or the termination drain.
    bool idle_flush_round_robin = true;
    /// A message that does not fit an empty aggregation buffer (local threshold) is merged into its destination's
    /// buffer, which is then sent at once as a large message (its own tag, received by probing into a buffer of the
    /// probed size). Costs every poll one extra MPI_Improbe for that tag. false: such a message throws.
    bool allow_large_messages = true;
    /// EXPERIMENT SWITCH (branch exp/pool-fix-switch, not for main): false restores the aggregation buffer pool
    /// handling from before #29 / #30, i.e. the overflow flush keeps the destination's entry and recycles its
    /// moved-from shell (the pool leaks about a buffer per overflow flush, the cap does not take effect), own
    /// messages may take the proxy's reserve, and own posts never wait for the reserve. For A/B measurements of the
    /// fix with one binary.
    bool bounded_pool = true;
    /// EXPERIMENT SWITCH: a flush of the destination being posted to keeps its map entry, and the moved-from shell
    /// left in it is dropped (never recycled, never merged into: the next post takes a pool buffer). This is the leak
    /// fix without #29's erase, which changes the termination drain's map order (KaCCv2: flat ms-async on lake-lanier
    /// +13-25 %). With bounded_pool = true: the pool fix with #30's reserve rules, minus the erase (the candidate).
    /// With bounded_pool = false: the legacy pool handling minus the leak; under indirection own messages may then
    /// still overdraw the proxy's reserve, so the pool is bounded there only by the old redirect ceiling.
    bool drop_shells = false;
    /// EXPERIMENT SWITCH: see DrainOrder.
    DrainOrder drain_order = DrainOrder::map;
    /// EXPERIMENT SWITCHES (branch exp/drain-policy): the behaviour changes since KaCCv2's base 5664f7f that are not
    /// already configurable, for a clean A/B in one binary. Together with idle_flush = off,
    /// allow_large_messages = false and bounded_pool = false they restore 5664f7f's behaviour for small messages.
    /// poll_while_waiting = false: poll_throttled skips polls even while buffers are parked or every send slot is
    /// taken (before 9c4bbf0, #26).
    bool poll_while_waiting = true;
    /// half_window_oversize_pass = false: a buffer larger than its credit passes only if it exceeds the whole credit
    /// window, not half of it (before b75b61f).
    bool half_window_oversize_pass = true;
    /// EXPERIMENT SWITCH (branch exp/split-forwarding): a proxy aggregates what it forwards in buffers of their own,
    /// separate from this rank's own messages to the same destination, and never parks a partially filled one (the
    /// termination drain, like the idle flush, sends it only if it can go out at once). Own messages then never enter
    /// the proxy's reserve, so own posts need not wait for it (#30's pause is off), and the reserve is bounded by the
    /// forwarded payload alone (see redirect_pool_ceiling). Costs aggregation: own and forwarded messages to the same
    /// destination travel in separate buffers.
    bool separate_forwarding = false;
    /// EXPERIMENT SWITCH (branch exp/split-forwarding): see FlowController::exact_redirect_limit.
    bool exact_redirect_limit = false;
    /// Credit budget per rank, in buffers per peer on average (see FlowController). nullopt or 0: a fixed window
    /// of num_credit_buffers per link. Otherwise links start at two buffers and grow up to num_credit_buffers while
    /// all inbound windows plus the forwarded payload held stay within credit_budget_buffers * #peers buffers; with
    /// indirection that is also the redirect limit.
    std::optional<std::size_t> credit_budget_buffers = std::nullopt;
    /// DIAGNOSTIC: classify withheld peers as feeders / bystanders of the congestion (num_withheld_bystanders etc.).
    /// Costs a map update per forwarded message (~11 % on lake-lanier), so off unless asked for.
    bool withhold_feeder_diagnostic = false;
    /// Window of links whose buffers are only delivered here (not forwarded), in buffers; nullopt: num_credit_buffers.
    /// With indirection these are the hops from the proxies to the final receiver; the proxy's bound depends only on
    /// num_credit_buffers (the window of the links it forwards from). Ignored with a credit budget.
    std::optional<std::size_t> num_delivery_credit_buffers = std::nullopt;
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
        idle_flush_ = effective_config_.idle_flush;
        idle_flush_round_robin_ = effective_config_.idle_flush_round_robin;
        queue_.allow_large_messages(effective_config_.allow_large_messages);
        bounded_pool_ = effective_config_.bounded_pool;
        drop_shells_ = effective_config_.drop_shells;
        reserve_aggregation_buffers(effective_config_.num_request_slots);
#ifdef BRIEFKASTEN_STALL_TRACE
        if (char const* trace = std::getenv("BRIEFKASTEN_STALL_TRACE_SECONDS")) {
            stall_trace_interval_ = std::strtod(trace, nullptr);
            stall_trace_last_ = std::chrono::steady_clock::now();
        }
#endif
        drain_order_ = effective_config_.drain_order;
        poll_while_waiting_ = effective_config_.poll_while_waiting;
        flow_.half_window_oversize_pass(effective_config_.half_window_oversize_pass);
        flow_.exact_redirect_limit(effective_config_.exact_redirect_limit);
        separate_forwarding_ = effective_config_.separate_forwarding;
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
        auto const budget_buffers = effective_config_.credit_budget_buffers.value_or(0);
        flow_.configure_budget(num_credit_buffers * buffer_size, num_peers, budget_buffers * buffer_size * num_peers,
                               buffer_size);
        flow_.delivery_window(effective_config_.num_delivery_credit_buffers.value_or(0) * buffer_size);
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
        // An own message waits while the proxy's reserve is in use beyond one buffer per peer, so the reserve
        // holds forwarded payload (apart from at most one partially filled buffer per destination) and stays
        // bounded by the redirect limit (see redirect_pool_ceiling). Waiting here is safe: only forwarding
        // posts from inside a handler (split_handler asserts it), and forwarding never waits, so every rank
        // keeps consuming and granting while it polls.
        // With separate_forwarding own payload never enters the reserve, so there is nothing to wait for.
        if (bounded_pool_ && !separate_forwarding_ && redirecting_depth_ == 0 &&
            reserve_buffers() > reserve_pause_threshold()) {
            num_reserve_waits_++;
            do {
                poll(on_message);
                progress_hook();
            } while (reserve_buffers() > reserve_pause_threshold());
        }
        auto ret = post_message_impl(
            std::forward<decltype(message)>(message), receiver, envelope_sender, envelope_receiver, tag,

            [&](auto /*it*/) {  // handle_overflow
                // by key, not iterator: polling may modify aggregation_buffers_
                resolve_overflow_blocking(buffer_key(receiver), on_message, progress_hook);
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
                bool success = resolve_overflow(buffer_key(receiver));
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
        bool flushed = false;
        // with separate_forwarding the destination also has a buffer of forwarded messages
        for (PEID key : {receiver, forwarding_key(receiver)}) {
            auto it = aggregation_buffers_.find(key);
            if (it != aggregation_buffers_.end()) {
                flushed = flush_buffer_impl(it).second || flushed;
            }
            if (!separate_forwarding_) {
                break;
            }
        }
        return flushed;
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

    /// Only every \p poll_skip_threshold-th call polls, unless buffers are parked or every send slot is taken:
    /// then every call polls, as only a poll notices completed sends and arriving grants, and a skipped poll
    /// leaves the parked buffers (and the sends behind them) waiting. Grants this rank owes others are still
    /// throttled.
    auto poll_throttled(MessageHandler<MessageType> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD)
        -> std::optional<std::pair<bool, bool>> {
        bool const waiting = poll_while_waiting_ && (parked_elements_ > 0 || !queue_.has_send_capacity());
        if (poll_skip_threshold > 1 && !waiting && (poll_throttle_count_++ % poll_skip_threshold) != 0) {
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
        // Ahead of every abort check: an attempt cancelled by an arrival still gets the caller's tail out.
        // The drain below runs only on attempts that reach a counting round, which under steady traffic
        // may be none for a long time, so without this a partially filled buffer waits for global quiet.
        flush_idle_buffers();
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
        if (drain_order_ != DrainOrder::map) {
            std::ranges::sort(drain_targets_);
            if (drain_order_ == DrainOrder::rotating) {
                std::ranges::rotate(drain_targets_, std::ranges::lower_bound(drain_targets_, drain_cursor_));
            }
        }
        num_drains_++;
        num_drain_targets_ += drain_targets_.size();
        auto finish = [&] { draining_ = false; };
        // new work arrived before target i was flushed: the rest waits for the next drain
        auto stop_early = [&](std::size_t i) {
            num_drain_early_stops_++;
            num_drain_targets_left_ += drain_targets_.size() - i;
            drain_cursor_ = drain_targets_[i];
            finish();
        };
        for (std::size_t i = 0; i < drain_targets_.size(); ++i) {
            PEID const target = drain_targets_[i];
            poll(on_message);  // observe arrivals (may flip should_stop) and progress sends
            progress_hook();
            if (should_stop()) {
                stop_early(i);
                return;
            }
            // with flow control a flush never fails (it parks), so there is nothing to wait for
            while (!flow_.enabled() && !queue_.has_send_capacity()) {
                num_drain_capacity_waits_++;
                poll(on_message);  // only block when slots are exhausted; polling frees them as peers receive
                progress_hook();
                if (should_stop()) {
                    stop_early(i);
                    return;
                }
            }
            drain_cursor_ = target + 1;
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
            // separate_forwarding: a partially filled forwarded buffer goes out only if it can be sent now. Parking
            // it would let a destination collect partially filled buffers drain after drain, and those are bounded
            // only per forwarded element. It stays pending, so termination waits for the next attempt.
            if (separate_forwarding_ && is_forwarding_key(target) && !it->second.empty() &&
                (!flow_.has_credit(destination(target), it->second.size()) || !queue_.has_send_capacity())) {
                num_drain_forward_deferred_++;
                continue;
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

    /// See IdleFlush. Takes effect at the next terminate() call.
    void idle_flush(IdleFlush mode) {
        idle_flush_ = mode;
        idle_last_size_.clear();
        idle_resume_.reset();
    }

    [[nodiscard]] IdleFlush idle_flush() const {
        return idle_flush_;
    }

    /// Buffers sent (or parked for credit) by the idle flush, and the elements they carried.
    [[nodiscard]] std::size_t num_idle_flushes() const {
        return num_idle_flushes_;
    }

    [[nodiscard]] std::size_t num_idle_flush_elements() const {
        return num_idle_flush_elements_;
    }

    /// Buffers IdleFlush::stale left in place because they grew since the previous attempt.
    [[nodiscard]] std::size_t num_idle_flush_skips() const {
        return num_idle_flush_skips_;
    }

    /// Buffers the idle flush left in place because their destination had no credit.
    [[nodiscard]] std::size_t num_idle_flush_deferred() const {
        return num_idle_flush_deferred_;
    }

    /// Idle flushes that ended early because no send slot was free; the rest waits for the next attempt.
    [[nodiscard]] std::size_t num_idle_flush_stops() const {
        return num_idle_flush_stops_;
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

    /// Aggregation buffers in the pool: free, filling, parked or in flight. At most the pool cap plus the proxy's
    /// overdraft (\ref max_num_aggregation_buffers, \ref redirect_overdraft).
    [[nodiscard]] std::size_t num_allocated_buffers() const {
        return num_aggregation_buffers_;
    }

    /// Pool buffers not in use right now; never more than \ref num_allocated_buffers.
    [[nodiscard]] std::size_t num_free_buffers() const {
        return free_aggregation_buffers_.size();
    }

    [[nodiscard]] std::size_t num_overflows() const {
        return num_overflows_;
    }

    /// Messages that did not fit an empty aggregation buffer and were sent with their buffer at once (see
    /// Config::allow_large_messages), and their elements.
    [[nodiscard]] std::size_t num_large_messages() const {
        return num_large_messages_;
    }
    [[nodiscard]] std::size_t num_large_message_elements() const {
        return num_large_message_elements_;
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

    /// Buffers in use beyond the pool cap right now: the reserve, which only the proxy draws from.
    [[nodiscard]] std::size_t reserve_buffers() const {
        auto const in_use = num_aggregation_buffers_ - free_aggregation_buffers_.size();
        return in_use > max_num_aggregation_buffers_ ? in_use - max_num_aggregation_buffers_ : 0;
    }

    /// The most buffers in use beyond the pool cap at once; at most \ref redirect_pool_ceiling.
    [[nodiscard]] std::size_t peak_reserve_buffers() const {
        return peak_reserve_buffers_;
    }

    /// Own posts (post_message_blocking outside a redirection handler) wait while the reserve holds more than
    /// this many buffers: one per peer.
    [[nodiscard]] std::size_t reserve_pause_threshold() const {
        return num_peers_;
    }

    /// Termination drains, drains that stopped early because new work arrived, destinations snapshotted by all
    /// drains, and destinations an early stop left unflushed (Config::drain_order experiment).
    [[nodiscard]] std::size_t num_drains() const {
        return num_drains_;
    }
    [[nodiscard]] std::size_t num_drain_early_stops() const {
        return num_drain_early_stops_;
    }
    [[nodiscard]] std::size_t num_drain_targets() const {
        return num_drain_targets_;
    }
    [[nodiscard]] std::size_t num_drain_targets_left() const {
        return num_drain_targets_left_;
    }

    /// Own posts that waited for the reserve to drain (see \ref reserve_pause_threshold).
    /// Moved-from shells dropped instead of recycled (Config::drop_shells).
    [[nodiscard]] std::size_t num_dropped_shells() const {
        return num_dropped_shells_;
    }

    [[nodiscard]] std::size_t num_reserve_waits() const {
        return num_reserve_waits_;
    }

    /// Forwarded buffers the termination drain left in place because they could not be sent at once
    /// (Config::separate_forwarding).
    [[nodiscard]] std::size_t num_drain_forward_deferred() const {
        return num_drain_forward_deferred_;
    }

    /// Upper bound on \ref redirect_overdraft and \ref peak_reserve_buffers, in buffers.
    ///
    /// With Config::separate_forwarding: own messages use at most the pool cap, so the reserve holds at most the
    /// forwarded buffers. Per destination, forwarded buffers are filled one after another; one that leaves before it
    /// overflowed (idle flush, termination drain) is sent at once and holds a send slot until it completes, every
    /// other one was flushed by the message that went into the next. So any two consecutive ones hold more than a
    /// buffer of forwarded payload, i.e. per destination at most 2 * (payload / buffer) + 1 buffers, plus those
    /// in send slots. Payload: at most redirect_high_water forwarded elements in all.
    ///
    /// Otherwise: own messages acquire
    /// only below the pool cap and are not merged while the reserve holds more than \ref reserve_pause_threshold
    /// buffers, so buffers acquired beyond that hold forwarded payload only. Per destination, two consecutive
    /// such buffers hold more than one buffer of it (the message that overflowed the first went into the
    /// second), and the proxy holds at most redirect_high_water forwarded elements. Partially filled buffers
    /// besides: one per destination being filled, one per destination left over from before the pause, and
    /// idle flushes, which only send what can go out at once (at most one per send slot).
    [[nodiscard]] std::size_t redirect_pool_ceiling() const {
        auto const buffer_size = std::max<std::size_t>(queue_.reserved_receive_buffer_size(), 1);
        auto const forwarded_buffers = (flow_.redirect_high_water() + buffer_size - 1) / buffer_size;
        if (separate_forwarding_) {
            return (2 * forwarded_buffers) + num_peers_ + effective_config_.num_request_slots;
        }
        return (2 * forwarded_buffers) + reserve_pause_threshold() + (2 * num_peers_) +
               effective_config_.num_request_slots + 2;
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

    /// The most credit outstanding on all inbound links at once, in elements: what peers could have sent this rank
    /// without waiting (a bound on its unexpected messages).
    [[nodiscard]] std::size_t peak_inbound_credit() const {
        return flow_.peak_inbound_credit();
    }

    /// Config::credit_budget_buffers: the budget in elements (0: fixed windows), and how often due grants grew a
    /// link's window or halved it because the budget was full.
    [[nodiscard]] std::size_t credit_budget_elements() const {
        return flow_.budget_elements();
    }

    [[nodiscard]] std::size_t num_window_grows() const {
        return flow_.num_window_grows();
    }

    [[nodiscard]] std::size_t num_window_shrinks() const {
        return flow_.num_window_shrinks();
    }

    /// DIAGNOSTIC: where a proxy's redirect budget (max_num_redirect_elements) is when it starts withholding a
    /// grant, in elements: credit granted to proxied peers and not yet used, and forwarded elements filling a
    /// buffer, parked without credit, parked without a send slot, or in flight (posted, send not completed).
    struct WithholdSnapshot {
        std::size_t credit = 0;
        std::size_t filling = 0;
        std::size_t parked_credit = 0;
        std::size_t parked_slot = 0;
        std::size_t in_flight = 0;
        void add(WithholdSnapshot const& o) {
            credit += o.credit;
            filling += o.filling;
            parked_credit += o.parked_credit;
            parked_slot += o.parked_slot;
            in_flight += o.in_flight;
        }
        void max(WithholdSnapshot const& o) {
            credit = std::max(credit, o.credit);
            filling = std::max(filling, o.filling);
            parked_credit = std::max(parked_credit, o.parked_credit);
            parked_slot = std::max(parked_slot, o.parked_slot);
            in_flight = std::max(in_flight, o.in_flight);
        }
    };

    [[nodiscard]] std::size_t num_withhold_samples() const {
        return withhold_samples_;
    }
    /// DIAGNOSTIC: withhold episodes by the withheld peer's share of forwarded payload that went to destinations
    /// with payload parked for credit at that moment: bystanders (< 10 %), mixed, feeders (>= 50 %), unknown (the
    /// peer had sent nothing to forward yet). Many bystanders: withholding from feeders first could help.
    [[nodiscard]] std::size_t num_withheld_bystanders() const {
        return num_withheld_bystanders_;
    }
    [[nodiscard]] std::size_t num_withheld_mixed() const {
        return num_withheld_mixed_;
    }
    [[nodiscard]] std::size_t num_withheld_feeders() const {
        return num_withheld_feeders_;
    }
    [[nodiscard]] std::size_t num_withheld_unknown() const {
        return num_withheld_unknown_;
    }
    [[nodiscard]] double withheld_share_sum() const {
        return withheld_share_sum_;
    }
    [[nodiscard]] WithholdSnapshot const& withhold_sum() const {
        return withhold_sum_;
    }
    [[nodiscard]] WithholdSnapshot const& withhold_max() const {
        return withhold_max_;
    }
    [[nodiscard]] std::size_t num_withhold_episodes() const {
        return flow_.num_withhold_episodes();
    }
    [[nodiscard]] double withheld_seconds() const {
        return flow_.withheld_seconds();
    }
    [[nodiscard]] double max_withheld_seconds() const {
        return flow_.max_withheld_seconds();
    }
    [[nodiscard]] std::size_t max_num_redirect_elements() const {
        return flow_.max_num_redirect_elements();
    }

    [[nodiscard]] std::size_t num_grants_received() const {
        return flow_.num_grants_received();
    }

    /// Buffers sent with less credit than their size, let through as larger than half the window (see
    /// FlowController::has_credit); counted once per send.
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
        num_large_messages_ = 0;
        num_large_message_elements_ = 0;
        num_elements_flushed_ = 0;
        num_buffer_stalls_ = 0;
        num_drain_capacity_waits_ = 0;
        num_overflow_capacity_waits_ = 0;
        num_forced_flushes_ = 0;
        num_forced_flush_elements_ = 0;
        num_drain_skips_ = 0;
        num_idle_flushes_ = 0;
        num_idle_flush_elements_ = 0;
        num_idle_flush_skips_ = 0;
        num_idle_flush_deferred_ = 0;
        num_idle_flush_stops_ = 0;
        num_parked_for_credit_ = 0;
        num_parked_for_capacity_ = 0;
        num_redirect_buffer_stalls_ = 0;
        num_reserve_waits_ = 0;
        num_drains_ = 0;
        num_drain_early_stops_ = 0;
        num_drain_targets_ = 0;
        num_drain_targets_left_ = 0;
        peak_reserve_buffers_ = 0;
        num_dropped_shells_ = 0;
        num_drain_forward_deferred_ = 0;
        withhold_samples_ = 0;
        withhold_sum_ = {};
        withhold_max_ = {};
        num_withheld_bystanders_ = 0;
        num_withheld_mixed_ = 0;
        num_withheld_feeders_ = 0;
        num_withheld_unknown_ = 0;
        withheld_share_sum_ = 0.0;
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

    /// Keys of aggregation_buffers_ (and of the per-buffer bookkeeping keyed like it): the destination rank, or with
    /// Config::separate_forwarding for a proxy's forwarded messages the destination plus the communicator size.
    [[nodiscard]] PEID forwarding_key(PEID receiver) const {
        return receiver + queue_.size();
    }

    [[nodiscard]] bool is_forwarding_key(PEID key) const {
        return key >= queue_.size();
    }

    [[nodiscard]] PEID destination(PEID key) const {
        return is_forwarding_key(key) ? key - queue_.size() : key;
    }

    /// The buffer a post to \p receiver goes into: posts from inside a redirection handler are forwards.
    [[nodiscard]] PEID buffer_key(PEID receiver) const {
        return separate_forwarding_ && redirecting_depth_ > 0 ? forwarding_key(receiver) : receiver;
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
    ///
    /// This rank's own messages use at most the pool cap: buffers beyond it are the proxy's overdraft, kept for
    /// redirected payload. Taking them for own messages would leave the proxy without a free buffer, so it would
    /// overdraw again, and the overdraft would grow with everything this rank parks instead of with what the
    /// credits bound.
    auto acquire_buffer() -> std::optional<BufferContainer> {
        bool const for_redirect = redirecting_depth_ > 0;
        auto const in_use = num_aggregation_buffers_ - free_aggregation_buffers_.size();
        bool const own_at_cap = bounded_pool_ && !for_redirect && in_use >= max_num_aggregation_buffers_;
        if (free_aggregation_buffers_.empty() || own_at_cap) {
            if (for_redirect && num_aggregation_buffers_ >= max_num_aggregation_buffers_) {
                KASSERT(!bounded_pool_ || redirect_overdraft_ < redirect_pool_ceiling(),
                        "proxy overdrew the buffer pool by " << redirect_overdraft_ << " buffers, past the "
                                                             << redirect_pool_ceiling()
                                                             << " that credits should have bounded it to");
                redirect_overdraft_++;
            }
            if (!own_at_cap && num_aggregation_buffers_ < buffer_limit()) {
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
        peak_reserve_buffers_ = std::max(peak_reserve_buffers_, reserve_buffers());
        KASSERT(!bounded_pool_ || !flow_.enabled() || reserve_buffers() <= redirect_pool_ceiling(),
                "the reserve holds " << reserve_buffers() << " buffers, more than the " << redirect_pool_ceiling()
                                     << " the redirect limit bounds it to");
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
        // redirecting_depth_ is the same after every poll below (nested handlers restore it), and so is the key
        PEID const key = buffer_key(receiver);
        auto it = aggregation_buffers_.find(key);
        if (it == aggregation_buffers_.end()) {
            auto buffer = get_new_buffer();
            // a redirection handler may have created the entry meanwhile
            it = aggregation_buffers_.find(key);
            if (it == aggregation_buffers_.end()) {
                std::tie(it, std::ignore) = aggregation_buffers_.emplace(key, std::move(buffer));
            } else {
                recycle_buffer(std::move(buffer));
            }
        }

        auto envelope =
            MessageEnvelope{std::forward<decltype(message)>(message), envelope_sender, envelope_receiver, tag};
        bool overflow = false;
        // The destination's buffer after a flush: a fresh one from the pool. May poll; `it` is valid afterwards.
        auto replace_flushed_buffer = [&] {
            auto buffer = get_new_buffer();  // may poll
            it = aggregation_buffers_.find(key);
            if (it == aggregation_buffers_.end()) {
                std::tie(it, std::ignore) = aggregation_buffers_.emplace(key, std::move(buffer));
            } else if (it->second.empty()) {
                // still empty: it owns a pool buffer, or with drop_shells the moved-from shell of the flushed buffer;
                // recycle it (recycle_buffer drops a shell) instead of overwriting it
                recycle_buffer(std::move(it->second));
                it->second = std::move(buffer);
            } else {
                // refilled by a redirection handler, keep its payload
                recycle_buffer(std::move(buffer));
            }
        };
        // drop_shells: an overflow flush keeps the destination's entry with the moved-from shell of the sent buffer.
        // A post that finds it (e.g. a proxy's redirection handler while the overflowing post polls) must not merge
        // into the shell, which would grow into a buffer the pool never counted; it takes a pool buffer instead and
        // the shell is dropped.
        if constexpr (requires { it->second.capacity(); }) {
            if (drop_shells_ && it->second.empty() && it->second.capacity() == 0) {
                replace_flushed_buffer();
            }
        }
        // A message that does not fit even an empty buffer: flushing first would not help (the fresh buffer is
        // just as small, and flushing an empty one is a no-op), so it is merged into the current buffer, which is
        // then sent at once, as a large message if it exceeds the receive buffers.
        bool large = false;
        // loop: while we poll, a redirection handler may refill this destination's buffer, so re-check
        while (true) {
            size_t estimated_new_buffer_size = 0;
            if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
                estimated_new_buffer_size =
                    merge.estimate_new_buffer_size(it->second, receiver, queue_.rank(), envelope);
            } else {
                estimated_new_buffer_size = it->second.size() + envelope.message.size();
            }
            auto const delta = estimated_new_buffer_size - it->second.size();
            if (!check_for_buffer_overflow(it->second, delta)) {
                break;
            }
            if (check_for_local_buffer_overflow(BufferContainer{}, delta)) {
                if (!queue_.large_messages_allowed()) {
                    throw std::runtime_error("Message of " + std::to_string(delta) +
                                             " buffer elements does not fit an aggregation buffer and large "
                                             "messages are disabled (Config::allow_large_messages).");
                }
                large = true;
                break;
            }
            overflow = true;
            num_overflows_++;
            handle_overflow(it);  // may poll, `it` is invalid afterwards
            replace_flushed_buffer();
        }
        auto& buffer = it->second;
        auto old_buffer_size = buffer.size();
        merge(buffer, receiver, queue_.rank(), std::move(envelope));
        auto new_buffer_size = buffer.size();
        auto const merged = new_buffer_size - old_buffer_size;
        global_buffer_size_ += merged;
        if (redirecting_depth_ > 0 && merged > 0) {
            redirected_in_buffer_[key] += merged;
            flow_.hold_redirected(merged);
            if (effective_config_.withhold_feeder_diagnostic) {
                auto& by_destination = forwarded_by_source_[redirect_source_];
                by_destination[receiver] += merged;
                forwarded_from_source_[redirect_source_] += merged;
            }
        }
        if (large) {
            num_large_messages_++;
            num_large_message_elements_ += merged;
            overflow = true;
            num_overflows_++;
            handle_overflow(it);  // may poll, `it` is invalid afterwards
            replace_flushed_buffer();
        }
        return overflow;
    }

    /// @return an iterator to the next buffer (and true), or the input iterator (and false) if flushing failed
    auto flush_buffer_impl(BufferMap::iterator buffer_it, bool erase = true)
        -> std::pair<typename BufferMap::iterator, bool> {
        KASSERT(buffer_it != aggregation_buffers_.end(), "Trying to flush non-existing buffer.");
        auto& [key, buffer] = *buffer_it;
        PEID const receiver = destination(key);
        if (idle_flush_ == IdleFlush::stale) {
            // whatever fills this destination next starts a new buffer, which IdleFlush::stale must see grow
            idle_last_size_.erase(key);
        }
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
            auto const episodes_before = flow_.num_withhold_episodes();
            flow_.release_redirected(take_redirected(key));
            sample_withhold(episodes_before);
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
                park_buffer(receiver, std::move(buffer_it->second), take_redirected(key));
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
        auto const redirected = take_redirected(key);
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

    /// The idle flush (see IdleFlush). Never blocks, never polls, and never parks: a buffer goes out only if it
    /// can be sent right now, otherwise it stays in place and keeps filling. Parking it would gain nothing (it
    /// waits for the same credit either way) and would let one destination accumulate several partially filled
    /// buffers -- credits bound elements, not buffers, so the pool, and a proxy's overdraft, would grow past
    /// what the credit window accounts for.
    void flush_idle_buffers() {
        if (idle_flush_ == IdleFlush::off || aggregation_buffers_.empty()) {
            return;
        }
        // Walk the buffer map as a ring, starting where the previous idle flush ran out of send slots. Each
        // step either advances or erases the current entry (unordered_map::erase invalidates nothing else and
        // never rehashes), and nothing polls, so nothing is inserted: `remaining` steps visit every entry that
        // existed on entry exactly once.
        auto it = aggregation_buffers_.begin();
        if (idle_flush_round_robin_ && idle_resume_) {
            if (auto resume = aggregation_buffers_.find(*idle_resume_); resume != aggregation_buffers_.end()) {
                it = resume;
            }
        }
        idle_resume_.reset();
        auto advance = [&](auto next) {
            return next == aggregation_buffers_.end() ? aggregation_buffers_.begin() : next;
        };
        for (std::size_t remaining = aggregation_buffers_.size(); remaining > 0; --remaining) {
            if (!queue_.has_send_capacity()) {
                num_idle_flush_stops_++;
                idle_resume_ = it->first;  // the rest waits for the next attempt or the termination drain
                return;
            }
            auto const size = it->second.size();
            if (size == 0) {
                // keep the entry and its pool buffer, the destination is likely to be used again
                it = advance(std::next(it));
                continue;
            }
            if (idle_flush_ == IdleFlush::stale) {
                // an unseen destination reads 0 and so counts as grown: every buffer waits at least one attempt
                auto& last_seen = idle_last_size_[it->first];
                if (size > last_seen) {
                    last_seen = size;
                    num_idle_flush_skips_++;
                    it = advance(std::next(it));
                    continue;
                }
            }
            if (flow_.enabled() && !flow_.has_credit(destination(it->first), size)) {
                num_idle_flush_deferred_++;
                it = advance(std::next(it));
                continue;
            }
            auto [next, flushed] = flush_buffer_impl(it, /*erase=*/true);
            KASSERT(flushed, "send capacity and credit were checked, so the flush must succeed");
            num_idle_flushes_++;
            num_idle_flush_elements_ += size;
            if (aggregation_buffers_.empty()) {
                return;
            }
            it = advance(next);
        }
    }

    /// \p redirected: the redirected elements in \p buffer (take_redirected).
    void park_buffer(PEID receiver, BufferContainer&& buffer, std::size_t redirected) {
        auto const elements = buffer.size();
        auto& queue_for_peer = parked_[receiver];
        if (queue_for_peer.empty()) {
            parked_peers_.push_back(receiver);
        }
        queue_for_peer.push_back(ParkedBuffer{.buffer = std::move(buffer), .redirected = redirected});
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

    /// DIAGNOSTIC: if a withhold episode began, record where the redirect budget is: outstanding credit to proxied
    /// peers, and the redirected elements filling, parked (for credit / for a send slot) and in flight.
    void sample_withhold(std::size_t episodes_before) {
        if (flow_.num_withhold_episodes() == episodes_before) {
            return;
        }
        WithholdSnapshot snap;
        snap.credit = flow_.sum_proxied_in_credit();
        for (auto const& entry : redirected_in_buffer_) {
            snap.filling += entry.second;
        }
        for (auto& [receiver, queue_for_peer] : parked_) {
            for (auto const& parked : queue_for_peer) {
                if (parked.redirected == 0) {
                    continue;
                }
                if (flow_.has_credit(receiver, parked.buffer.size())) {
                    snap.parked_slot += parked.redirected;
                } else {
                    snap.parked_credit += parked.redirected;
                }
            }
        }
        for (auto const& entry : redirected_by_receipt_) {
            snap.in_flight += entry.second;
        }
        withhold_samples_++;
        withhold_sum_.add(snap);
        withhold_max_.max(snap);
        // Is a withheld peer feeding the congestion, or a bystander? Congested: destinations with a forwarded
        // buffer parked for credit. A peer's share: of what it sent through this proxy so far, the part that went
        // to congested destinations.
        if (!effective_config_.withhold_feeder_diagnostic) {
            flow_.take_new_episode_peers();
            return;
        }
        congested_scratch_.clear();
        for (auto& [receiver, queue_for_peer] : parked_) {
            for (auto const& parked : queue_for_peer) {
                if (parked.redirected > 0 && !flow_.has_credit(receiver, parked.buffer.size())) {
                    congested_scratch_.push_back(receiver);
                    break;
                }
            }
        }
        for (PEID peer : flow_.take_new_episode_peers()) {
            auto const total_it = forwarded_from_source_.find(peer);
            if (total_it == forwarded_from_source_.end() || total_it->second == 0) {
                num_withheld_unknown_++;
                continue;
            }
            std::size_t to_congested = 0;
            auto const& by_destination = forwarded_by_source_[peer];
            for (PEID destination : congested_scratch_) {
                auto const it = by_destination.find(destination);
                to_congested += it == by_destination.end() ? 0 : it->second;
            }
            auto const share = static_cast<double>(to_congested) / static_cast<double>(total_it->second);
            withheld_share_sum_ += share;
            if (share < 0.1) {
                num_withheld_bystanders_++;
            } else if (share < 0.5) {
                num_withheld_mixed_++;
            } else {
                num_withheld_feeders_++;
            }
        }
    }

    /// Redirected elements in the current buffer of \p key (see buffer_key); resets the count, since the buffer is
    /// being sent or parked.
    std::size_t take_redirected(PEID key) {
        auto it = redirected_in_buffer_.find(key);
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
        BufferMap::iterator current_buffer,  // kept (not erased) only without bounded_pool_
        PreFlushHook&& pre_flush_hook,       // NOLINT(cppcoreguidelines-missing-std-forward)
        PostFlushHook&& post_flush_hook,     // NOLINT(cppcoreguidelines-missing-std-forward)
        bool break_when_flush_fails = true) {
        auto it = aggregation_buffers_.begin();
        bool flushed_something = false;
        while (it != aggregation_buffers_.end()) {
            pre_flush_hook();
            bool current_flush_successful = false;
            std::tie(it, current_flush_successful) =
                flush_buffer_impl(it, erase_on_flush() || it != current_buffer);  // `it` is updated by std::tie
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
            auto it = flush_buffer_impl(largest_buffer, erase_on_flush() || largest_buffer != current_buffer);
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
            auto const outer_redirect_source = redirect_source_;
            if (redirects) {
                redirect_source_ = source;
            }
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
            redirect_source_ = outer_redirect_source;
            redirecting_depth_ -= redirects ? 1 : 0;
            auto const episodes_before = flow_.num_withhold_episodes();
            flow_.track_receive(source, elements);
            sample_withhold(episodes_before);
            // Handlers must not send, except for the forwarding by the proxy. Otherwise flow control can deadlock.
            KASSERT(may_redirect(source) || num_posts_ == posts_before,
                    "posted " << (num_posts_ - posts_before) << " message(s) while handling a buffer from rank "
                              << source
                              << ", which is not a may_redirect peer. Either the indirection scheme is wrong, or the "
                                 "application sends from inside a message handler.");
        };
    }

    /// Return a buffer that was never sent to the pool. Only pool buffers may come here: a flush that sends or
    /// parks a buffer erases its destination's entry, so no moved-from shell is left to be recycled.
    /// Whether a flush of the destination being posted to (overflow, flush-all, flush-largest) erases its entry:
    /// bounded_pool does (#29), so no moved-from shell is left; drop_shells keeps the entry and drops the shell.
    [[nodiscard]] bool erase_on_flush() const {
        return bounded_pool_ && !drop_shells_;
    }

    void recycle_buffer(BufferContainer&& buffer) {
        if constexpr (requires { buffer.capacity(); }) {
            // a pool buffer keeps its capacity, so capacity 0 means a moved-from shell
            if (drop_shells_ && buffer.capacity() == 0) {
                num_dropped_shells_++;
                return;
            }
        }
        num_buffer_recycles_++;
        buffer.resize(0);  // this does not reduce the capacity
        if constexpr (requires { buffer.capacity(); }) {
            // a large message grew it: do not keep that memory in the pool
            auto const reserved = queue_.reserved_receive_buffer_size();
            if (buffer.capacity() > 2 * reserved) {
                buffer = BufferContainer{};
                buffer.reserve(reserved);
            }
        }
        free_aggregation_buffers_.emplace_back(std::move(buffer));
        KASSERT((!bounded_pool_ && !drop_shells_) || free_aggregation_buffers_.size() <= num_aggregation_buffers_,
                "the free list holds " << free_aggregation_buffers_.size() << " buffers, more than the pool's "
                                       << num_aggregation_buffers_ << ": a buffer was recycled twice or never counted");
    }

    /// Called when a send completed; releases its redirected elements.
    auto reclaim_aggregation_buffer(std::size_t receipt, BufferContainer&& buffer) {
        num_buffer_reclaims_++;
        auto it = redirected_by_receipt_.find(receipt);
        if (it != redirected_by_receipt_.end()) {
            auto const episodes_before = flow_.num_withhold_episodes();
            flow_.release_redirected(it->second);
            sample_withhold(episodes_before);
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
                // Erase the entry: a sent or parked buffer leaves a moved-from shell, which must not reach the
                // pool (it is not a pool buffer, and filling it allocates one the pool never counted).
                // replace_flushed_buffer looks the destination up again and gives it a pool buffer.
                auto ret = flush_buffer_impl(current_buffer, /*erase=*/erase_on_flush());
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
    std::size_t num_large_messages_ = 0;
    std::size_t num_large_message_elements_ = 0;
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
    IdleFlush idle_flush_ = IdleFlush::all;
    std::unordered_map<PEID, std::size_t> idle_last_size_;  ///< IdleFlush::stale: size at the previous attempt
    std::size_t num_idle_flushes_ = 0;
    std::size_t num_idle_flush_elements_ = 0;
    std::size_t num_idle_flush_skips_ = 0;
    std::size_t num_idle_flush_deferred_ = 0;
    std::size_t num_idle_flush_stops_ = 0;
    bool idle_flush_round_robin_ = true;
    std::optional<PEID> idle_resume_;  ///< destination the previous idle flush stopped at (round robin)
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
    std::size_t withhold_samples_ = 0;   ///< DIAGNOSTIC: calls that began at least one withhold episode
    WithholdSnapshot withhold_sum_;      ///< DIAGNOSTIC: summed over the samples
    WithholdSnapshot withhold_max_;      ///< DIAGNOSTIC: componentwise maximum over the samples
    PEID redirect_source_ = -1;          ///< the proxied peer whose buffer is being handled (forwards come from it)
    /// DIAGNOSTIC: forwarded elements per proxied source and destination, and per source
    std::unordered_map<PEID, std::unordered_map<PEID, std::size_t>> forwarded_by_source_;
    std::unordered_map<PEID, std::size_t> forwarded_from_source_;
    std::vector<PEID> congested_scratch_;
    std::size_t num_withheld_bystanders_ = 0;  ///< DIAGNOSTIC: episodes of peers with < 10 % to congested destinations
    std::size_t num_withheld_mixed_ = 0;       ///< 10-50 %
    std::size_t num_withheld_feeders_ = 0;     ///< >= 50 %
    std::size_t num_withheld_unknown_ = 0;     ///< the peer has not sent anything to forward yet
    double withheld_share_sum_ = 0.0;          ///< sum of the shares (divide by the three classified counts)
    std::size_t num_parked_for_credit_ = 0;
    std::size_t num_parked_for_capacity_ = 0;
    std::size_t num_redirect_buffer_stalls_ = 0;
    std::size_t redirect_overdraft_ = 0;
    DrainOrder drain_order_ = DrainOrder::map;  ///< Config::drain_order (experiment switch)
    bool poll_while_waiting_ = true;            ///< Config::poll_while_waiting (experiment switch)
    PEID drain_cursor_ = 0;                     ///< DrainOrder::rotating: where the next drain starts
    std::size_t num_drains_ = 0;
    std::size_t num_drain_early_stops_ = 0;
    std::size_t num_drain_targets_ = 0;
    std::size_t num_drain_targets_left_ = 0;
    std::size_t num_reserve_waits_ = 0;     ///< own posts that waited for the reserve to drain
    std::size_t peak_reserve_buffers_ = 0;  ///< most buffers in use beyond the pool cap at once
    std::size_t num_peers_ = 0;
    bool bounded_pool_ = true;  ///< Config::bounded_pool (experiment switch)
    bool drop_shells_ = false;  ///< Config::drop_shells (experiment switch)
    std::size_t num_dropped_shells_ = 0;
    bool separate_forwarding_ = false;  ///< Config::separate_forwarding (experiment switch)
    std::size_t num_drain_forward_deferred_ = 0;
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
