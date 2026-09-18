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
    /// Memory this queue lets peers have in flight towards it, rationed out as credit (see
    /// internal::FlowController). nullopt means "whatever this context defaults to": off for a flat queue,
    /// DEFAULT_FLOW_CONTROL_BUDGET_BYTES under IndirectionAdapter, which is where relaying -- and so the
    /// blocking-inside-a-handler failure credit exists to remove -- happens at all. An explicit 0 turns it
    /// off everywhere, which is the A/B control.
    std::optional<std::size_t> flow_control_budget_bytes = std::nullopt;
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
          flow_(comm, kamping::Environment<>::tag_upper_bound() - 3, effective_config_.num_request_slots),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)),
          flush_strategy_(effective_config_.flush_strategy) {
        reserve_aggregation_buffers(effective_config_.num_request_slots);
        if (char const* trace = std::getenv("BRIEFKASTEN_STALL_TRACE_SECONDS")) {
            stall_trace_interval_ = std::strtod(trace, nullptr);
            stall_trace_last_ = std::chrono::steady_clock::now();
        }
        // Flat topology: every peer is a potential destination and nothing is ever relayed. Only fires if
        // the caller asked for flow control explicitly; IndirectionAdapter re-rations on top of this.
        if (effective_config_.flow_control_budget_bytes.value_or(0) > 0) {
            int comm_size = 0;
            MPI_Comm_size(comm, &comm_size);
            enable_flow_control(effective_config_.flow_control_budget_bytes.value(),
                                static_cast<std::size_t>(comm_size), /*has_relay_peers=*/false);
        }
    }

    /// Ration \p budget_bytes of in-flight payload across \p num_peers peers and, if this queue relays,
    /// reserve the buffer pool the relay needs to stay non-blocking.
    ///
    /// \p has_relay_peers is what separates the two topologies. Without relaying, an arriving packet is
    /// consumed by the handler and the pool only has to serve the application. With relaying, the handler
    /// must be able to acquire a buffer to forward into -- if it cannot, it spins, and a spinning handler
    /// is a receive slot that stays disarmed, which is the original defect. So the pool is sized to hold
    /// the whole rationed budget plus one partially filled buffer per destination, and that share is
    /// fenced off from the application (see acquire_buffer). The bound is exact rather than hopeful: the
    /// base windows and the shared pool sum to the budget, so relayed payload in flight can never exceed
    /// it, and payload spread over at most fan_out destinations needs at most budget/packet + fan_out
    /// buffers to hold.
    void enable_flow_control(std::size_t budget_bytes, std::size_t num_peers, bool has_relay_peers) {
        if (budget_bytes == 0) {
            return;
        }
        auto const packet_elements = std::max<std::size_t>(queue_.reserved_receive_buffer_size(), 1);
        auto const budget_elements = std::max<std::size_t>(budget_bytes / sizeof(BufferType), packet_elements);
        flow_.configure(budget_elements, packet_elements, num_peers);
        // Sized from the controller's own high-water mark, not from the nominal budget. The two differ
        // by one window, because the grant that trips the gate has already raised a peer's allowance by
        // then -- and getting this wrong does not deadlock, it silently degrades: the relay fails to
        // acquire a buffer, spins in get_new_buffer, and is blocking inside a handler again. The
        // `+ num_peers` covers one partially filled buffer per destination on top of the whole packets.
        relay_buffer_reserve_ =
            has_relay_peers ? (flow_.relay_high_water() / packet_elements) + num_peers + 1 : 0;
        // The APPLICATION needs a deferral allowance of its own, and this is where the first version got
        // it badly wrong: it gave the relay the whole worst-case reserve and left the application the
        // allowance it had before flow control existed (fan_out + slots + backlog = 18 buffers at p=5).
        // But parking is a NEW consumer of application buffers -- before credits, a full buffer was
        // handed to MPI immediately and came straight back. Three parked packets plus the open buffers
        // and the in-flight sends exhaust 18, after which post_message_blocking spins in get_new_buffer
        // forever: measured as a hard stall on every rank count from 5 to 16, with credit sitting
        // unused on every peer.
        //
        // So the application gets the same allowance as the relay. Buffers are allocated lazily, so a
        // cap only costs what is actually used -- and what is actually used is bounded by credit on the
        // relay side and by the application blocking on this pool on its own side.
        auto const application_allowance =
            relay_buffer_reserve_ + num_peers + effective_config_.num_request_slots +
            effective_config_.send_backlog_capacity.value();
        max_num_aggregation_buffers(relay_buffer_reserve_ + application_allowance);
        if (stall_trace_interval_ > 0.0) {
            std::fprintf(stderr,
                         "[bk-config rank %d] budget_bytes=%zu budget_elems=%zu packet=%zu peers=%zu "
                         "relay=%d high_water=%zu reserve=%zu window=%zu\n",
                         rank(), budget_bytes, budget_elements, packet_elements, num_peers,
                         static_cast<int>(has_relay_peers), flow_.relay_high_water(),
                         relay_buffer_reserve_, flow_.base_window());
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
        stall_trace_tick();  // every spin loop in this class polls, so this is where a stall is visible
        // Grants first: a credit that arrived this poll may release a deferred packet in the same poll.
        flow_.poll();
        auto result = queue_.poll(split_handler(on_message), [&](std::size_t receipt, BufferContainer buffer) {
            reclaim_aggregation_buffer(receipt, std::move(buffer));
        });
        drain_deferred();
        return result;
    }

    /// Throttled poll. The throttle is counted HERE rather than delegated to the underlying queue, so
    /// that everything a poll drives -- the grant channel and the deferred queues included -- is
    /// throttled together.
    ///
    /// This matters more than it looks. The callers poll once per VERTEX (see KaCCv2's
    /// reachability_labeling and the async min_label_propagation), which is why the throttle exists at
    /// all: at skip 100 that is one MPI call per hundred vertices instead of one per vertex. Driving the
    /// flow controller ahead of the throttle put an MPI_Testsome over the grant slots back on every
    /// single call -- a hundredfold increase in MPI calls on the hottest loop of the async paths.
    /// Invisible on a small local graph, fatal at scale.
    ///
    /// Grants are safe to throttle for the same reason data receives are: nothing blocks on one
    /// arriving, it only decides how soon a parked packet can go out. Termination drives the controller
    /// unthrottled anyway (see terminate), so a run winding down does not wait on this.
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
    /// Periodic dump of everything that could be holding termination up. Off unless
    /// BRIEFKASTEN_STALL_TRACE_SECONDS is set, in which case it prints at most that often per queue.
    ///
    /// Exists because briefkasten stalls are hard to catch any other way: they are rare, they are
    /// timing-dependent, and by the time a run has hung there is nothing to look at -- the counters are
    /// only reported when a phase ENDS, which is exactly what is not happening. Print two dumps a few
    /// seconds apart and read what has not moved between them.
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
            << " buffered=" << global_buffer_size_ << " deferred=" << deferred_elements_
            << " deferred_peers=" << deferred_peers_.size() << " buffers=" << num_aggregation_buffers_
            << "/" << max_num_aggregation_buffers_ << " free=" << free_aggregation_buffers_.size()
            << " relay_reserve=" << relay_buffer_reserve_ << " credit_deferrals=" << num_credit_deferrals_
            << " relay_buffer_stalls=" << num_relay_buffer_stalls_
            << " buffer_stalls=" << num_buffer_stalls_ << " sends=" << counts.send
            << " recvs=" << counts.receive << " term_calls=" << queue_.num_terminate_calls()
            << " term_drains=" << queue_.num_termination_drains()
            << " term_rounds=" << num_termination_rounds() << " polls=" << num_polls()
            << " unproductive=" << num_unproductive_polls() << " deaf=" << num_deaf_probes()
            << " overflow_waits=" << num_overflow_capacity_waits()
            << " drain_waits=" << num_drain_capacity_waits()
            << " | acquires=" << num_buffer_acquires_ << " recycles=" << num_buffer_recycles_
            << " reclaims=" << num_buffer_reclaims_ << "\n    " << flow_.describe();
        for (auto const& entry : deferred_) {
            std::size_t elements = 0;
            for (auto const& packet : entry.second) {
                elements += packet.buffer.size();
            }
            out << "\n    DEFERRED to " << entry.first << ": " << entry.second.size() << " packets, "
                << elements << " elements";
        }
        for (auto const& entry : aggregation_buffers_) {
            if (!entry.second.empty()) {
                out << "\n    buffering for " << entry.first << ": " << entry.second.size() << " elements";
            }
        }
        out << "\n";
        std::fputs(out.str().c_str(), stderr);
    }

    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message, std::invocable<> auto&& progress_hook) {
        stall_trace_tick();
        // MessageQueue::terminate's counting loop polls the RAW queue, not this one, so on its own it
        // drives neither the grant channel nor the deferred queues -- and a parked packet keeps
        // pending_elements() non-zero, so termination would never fire. Nothing would ever unpark it
        // either, because unparking needs a grant and grants arrive through flow_.poll(). The progress
        // hook is the one callback that loop does invoke every iteration, so the credit machinery rides
        // on it. Without this the indirect alltoall test hangs outright.
        auto drive = [&] {
            flow_.poll();
            drain_deferred();
            progress_hook();
        };
        // Transport progress on every attempt, but NO drain here. See prepare_and_count.
        auto before_next_message_counting_round_hook = [&] { drive(); };
        // The drain runs HERE, fused with the counts snapshot, and that position is load-bearing.
        //
        // This hook's sibling above runs at the TOP of terminate's loop, ahead of both early-abort
        // checks, so a drain placed there is paid by every attempt -- and nearly every attempt is
        // cancelled by an arriving message. Measured on rmat n18 p128: 8,379 terminate() calls per rank
        // per iteration against 3 that reached an allreduce. Everything this loop flushes is FORCED, at
        // whatever fill it happens to have, so draining on all 8,379 fragments the traffic badly: 64% of
        // relay packets at p=608 went out at ~5% fill.
        //
        // The two-queue version had this right and the one-queue collapse nearly threw it away. There,
        // the relay hop was drained from extra_round_prepare (fused with the counts, so ~3 times) while
        // only the originating hop paid the per-attempt drain. With a single queue the two hops share a
        // buffer set, so leaving the drain in the per-attempt hook would have force-flushed the relay's
        // buffers 8,379 times an iteration instead of 3 -- reintroducing exactly the fragmentation that
        // commits da23c11 and 17700dd removed.
        //
        // Safe because flushing is about progress, not correctness: the `pending` term below refuses
        // termination while any payload is buffered, so a buffer left un-drained can delay termination
        // but can never let it fire with data undelivered. Before that term existed this would have been
        // a silent-data-loss bug.
        //
        // Our own buffered payload -- including anything a relay received and merged but has not yet
        // forwarded -- joins the counting round. send/receive are counted per PACKET, so without this
        // term a relayed message sitting in a proxy's buffer is invisible: the packet that carried it was
        // sent once and received once, the counts balance, and termination fires with data undelivered.
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
        // An empty buffer map with payload still on the books means a buffer was destroyed without its
        // contents being accounted for -- and the consequence is not just lost data: pending never
        // returns to zero, so termination can never fire and the run hangs with no other symptom. That
        // is exactly how the tight-budget stall presented, so it is asserted rather than left to be
        // rediscovered. Compiled out at assertion level 0.
        KASSERT(!aggregation_buffers_.empty() || global_buffer_size_ == 0,
                "buffer map is empty but " << global_buffer_size_
                                           << " elements are still counted as buffered");
        // Deferred packets count too. They have left their aggregation buffer but have not been handed to
        // MPI, so neither global_buffer_size_ nor the send count sees them; without this term termination
        // could fire with a parked packet still undelivered -- the same silent-loss shape that
        // MessageCounter::pending exists to close, arriving by a new route.
        return global_buffer_size_ + deferred_elements_;
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
            // Under flow control a flush never fails -- it parks the packet in its destination's deferred
            // queue -- so there is nothing to wait for and the drain runs to completion without blocking.
            // pending_elements() counts what was parked, so termination still refuses until it is gone.
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

    /// Payload parked because its destination had granted no room (or our request pool was busy). The
    /// number to read against runtime: it is what the protocol costs, where \ref num_send_capacity_waits
    /// was what having no protocol cost. A deferral is cheap -- it is a move and a poll away from being
    /// sent -- whereas a capacity wait was a spin inside a receive handler.
    [[nodiscard]] std::size_t num_credit_deferrals() const {
        return num_credit_deferrals_;
    }

    /// Packets parked right now, in elements. Should be near zero except under genuine congestion.
    [[nodiscard]] std::size_t deferred_elements() const {
        return deferred_elements_;
    }

    /// Relayed payload occupying the relay reserve, in elements. Bounded by the configured budget.
    [[nodiscard]] std::size_t relay_outstanding_elements() const {
        return flow_.relay_outstanding();
    }

    /// Should stay at zero; see acquire_buffer. Non-zero means the relay could not get a buffer and blocked
    /// inside a handler, i.e. the pool sizing argument has broken.
    [[nodiscard]] std::size_t num_relay_buffer_stalls() const {
        return num_relay_buffer_stalls_;
    }

    /// See FlowController::num_grants_withheld.
    [[nodiscard]] std::size_t num_grants_withheld() const {
        return flow_.num_grants_withheld();
    }

    [[nodiscard]] std::size_t num_grants_sent() const {
        return flow_.num_grants_sent();
    }

    [[nodiscard]] std::size_t num_grants_received() const {
        return flow_.num_grants_received();
    }

    /// Times a peer's window was grown out of the shared pool because it had consumed its whole outstanding
    /// grant before we could re-grant, i.e. how much skew the pool actually absorbed.
    [[nodiscard]] std::size_t num_window_grows() const {
        return flow_.num_window_grows();
    }

    /// Packets larger than a whole window, let through on the escape hatch in FlowController::has_credit.
    /// Persistently non-zero means the aggregation threshold is mis-sized against the flow-control budget.
    [[nodiscard]] std::size_t num_oversize_passes() const {
        return flow_.num_oversize_passes();
    }

    [[nodiscard]] bool flow_control_enabled() const {
        return flow_.enabled();
    }

    [[nodiscard]] std::size_t flow_control_window() const {
        return flow_.base_window();
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
        num_credit_deferrals_ = 0;
        num_relay_buffer_stalls_ = 0;
        flow_.reset_counters();
        queue_.reset_counters();
    }

private:
    using BufferMap = std::unordered_map<PEID, BufferContainer>;
    using BufferList = std::vector<BufferContainer>;

    /// A packet that has left its aggregation buffer but has no credit to go out on yet. \c relayed is how
    /// much of it came in over a relay link, so the reserve can be released when its send completes.
    struct DeferredPacket {
        BufferContainer buffer;
        std::size_t relayed = 0;
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

    /// \return a free buffer, or nullopt if the caller must wait for one.
    ///
    /// When this queue relays, part of the pool is fenced off for the relay. A relay handler that cannot
    /// get a buffer spins, and a spinning handler is a disarmed receive slot -- the defect this whole
    /// change exists to remove -- so the application is held back at a lower cap than the relay is. The
    /// fence is sized in enable_flow_control so that the relay provably cannot exhaust its share.
    auto acquire_buffer() -> std::optional<BufferContainer> {
        bool const for_relay = relaying_depth_ > 0;
        if (!for_relay && relay_buffer_reserve_ > 0) {
            // What the relay could still obtain: buffers sitting free, plus buffers we have not created
            // yet. Both terms are exact and non-negative.
            //
            // NOT `num_aggregation_buffers_ - free_aggregation_buffers_.size()`, which is what this was
            // and which deadlocked: those two counters are NOT conserved against each other. A flush
            // with erase=false moves the buffer out and leaves a moved-from husk in the map; the next
            // post merges straight into the husk, growing a buffer that was never acquired, and when
            // that is eventually flushed and reclaimed it enters the free list as a phantom. Measured on
            // a 5-rank gnm run: 24 buffers ever created against 97 in the free list, so the subtraction
            // underflowed to ~1.8e19 and the application was refused a buffer forever -- 10M failed
            // acquisitions per second, with nothing else moving.
            //
            // The husk pattern predates this code and was harmless until something compared the two
            // counters. It is fixed at the source below (resolve_overflow erases rather than leaving a
            // husk), but this check no longer depends on that holding.
            auto const creatable = max_num_aggregation_buffers_ -
                                   std::min(max_num_aggregation_buffers_, num_aggregation_buffers_);
            auto const available = free_aggregation_buffers_.size() + creatable;
            if (available <= relay_buffer_reserve_) {
                if (stall_trace_interval_ > 0.0 && (num_buffer_stalls_ % 2000000) == 0) {
                    std::fprintf(stderr,
                                 "[bk-quota rank %d] REFUSE app buffer: available=%zu reserve=%zu free=%zu "
                                 "created=%zu max=%zu map=%zu deferred_elems=%zu stalls=%zu\n",
                                 rank(), available, relay_buffer_reserve_, free_aggregation_buffers_.size(),
                                 num_aggregation_buffers_, max_num_aggregation_buffers_,
                                 aggregation_buffers_.size(), deferred_elements_, num_buffer_stalls_);
                }
                flush_largest_buffer();
                num_buffer_stalls_++;
                return std::nullopt;
            }
        }
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
                // Should be unreachable while flow control is on: the relay's share of the pool is sized
                // to hold the entire rationed budget plus one partial buffer per destination, and the
                // windows sum to that budget. If it ever fires, the sizing argument in
                // enable_flow_control is wrong and the relay is about to block inside a handler again.
                num_relay_buffer_stalls_ += for_relay ? 1 : 0;
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
            // Re-look-up, for the same reason the overflow path below does. get_new_buffer POLLS when the
            // pool is empty, and a poll runs a relay handler that can post to this very destination and
            // create the entry we just failed to find. insert_or_assign would then destroy the payload
            // the relay had merged -- silently, and worse than silently: global_buffer_size_ goes on
            // counting it, so pending_elements() never returns to zero and termination can NEVER fire.
            //
            // Needs a tight buffer pool to reach, because get_new_buffer only polls when it cannot
            // satisfy the request outright. That is why it hid behind the shipped 8 MiB budget and only
            // surfaced at --briefkasten-flow-control-budget-bytes 8192.
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
        // A LOOP, and the three-way reconciliation below is the point of it. Both calls inside can poll,
        // and a poll runs a relay handler that posts into this very queue -- possibly into this very
        // destination's buffer. Overwriting the entry afterwards (which is what the straight-line version
        // did, harmlessly, while the relay lived on a second queue object) discards whatever the relay
        // merged in: silent message loss, plus a global_buffer_size_ that keeps counting payload nobody
        // holds, so termination never fires. That is not hypothetical -- it cost 5,415 of 800,000 messages
        // per run on the four-rank indirect alltoall, nondeterministically.
        //
        // So each pass re-reads the entry and re-tests the overflow. The loop terminates because every
        // pass flushes (or parks) this destination's buffer, and the only thing that can refill it is a
        // relay handler forwarding payload that was already admitted under a credit.
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
            handle_overflow(it);             // customization point; may poll -> `it` is dead after this
            auto buffer = get_new_buffer();  // may poll too, for the same reason
            it = aggregation_buffers_.find(receiver);
            if (it == aggregation_buffers_.end()) {
                // A nested flush erased it (the drain and flush_largest_buffer both erase).
                std::tie(it, std::ignore) = aggregation_buffers_.emplace(receiver, std::move(buffer));
            } else if (it->second.empty()) {
                // The moved-from shell our own flush left behind. It carries no capacity, so swapping in
                // the fresh buffer is what the straight-line version did and is still right.
                it->second = std::move(buffer);
            } else {
                // Refilled by a relay handler while we polled. Its payload must survive, so the fresh
                // buffer goes back to the pool and the next pass re-tests against the real contents.
                recycle_buffer(std::move(buffer));
            }
        }
        // Read immediately before the merge, so that whatever a nested poll did to OTHER buffers (and to
        // global_buffer_size_) is already accounted for and this delta stays correct.
        auto& buffer = it->second;
        auto old_buffer_size = buffer.size();
        merge(buffer, receiver, queue_.rank(), std::move(envelope));
        auto new_buffer_size = buffer.size();
        auto const merged = new_buffer_size - old_buffer_size;
        global_buffer_size_ += merged;
        if (relaying_depth_ > 0 && merged > 0) {
            relayed_in_buffer_[receiver] += merged;
            flow_.note_relayed(merged);
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
                // Back to the pool, not destroyed. An entry that is empty here usually still owns a real
                // buffer with its capacity reserved, and dropping it silently shrank the pool: measured
                // at 68 buffers lost out of 74 on a tight-budget run, after which the application could
                // never acquire another one.
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
            // The BufferCleaner discarded the whole packet, relayed payload included. Nothing will ever
            // complete a send for it, so the reserve has to be released here or it leaks for the phase.
            flow_.note_relay_released(take_relayed(receiver));
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
        // THE GATE, and section 4.4 puts it here rather than at post_message deliberately: this is the
        // granularity at which the wire is actually used, so a credit is spent on a packet rather than on
        // every message merged into one.
        //
        // Under flow control a flush NEVER fails. If the peer has not made room, or our own request pool
        // is busy, the packet is parked in this destination's deferred queue and poll() sends it when
        // credit arrives. That is what lets the caller stop spinning, and it is the whole mechanism: a
        // relay handler that only ever appends and parks cannot block, so its receive slot is re-armed
        // immediately and it never goes deaf to its row. The park is also per destination, which is the
        // other half -- the old single FIFO backlog let a packet for a slow destination head-of-line
        // block every packet behind it regardless of where they were going.
        if (flow_.enabled()) {
            if (!flow_.has_credit(receiver, elements) || !queue_.has_send_capacity()) {
                num_credit_deferrals_++;
                defer_packet(receiver, std::move(buffer_it->second));
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
        auto const relayed = take_relayed(receiver);
        auto receipt = queue_.post_message(std::move(buffer_it->second), receiver);
        KASSERT(receipt.has_value(),
                "We checked before that there is capacity, so posting the message should not fail.");
        flow_.note_sent(receiver, elements);
        if (relayed > 0) {
            relayed_by_receipt_[*receipt] = relayed;
        }
        global_buffer_size_ -= pre_cleanup_buffer_size;
        if (erase) {
            return {aggregation_buffers_.erase(buffer_it), true};
        }
        return {++buffer_it, true};
    }

    /// Park a packet that has no credit (or no free request slot) in its destination's own queue.
    void defer_packet(PEID receiver, BufferContainer&& buffer) {
        auto const elements = buffer.size();
        auto& queue_for_peer = deferred_[receiver];
        if (queue_for_peer.empty()) {
            deferred_peers_.push_back(receiver);  // worklist entry; only added on the empty->non-empty edge
        }
        queue_for_peer.push_back(DeferredPacket{.buffer = std::move(buffer), .relayed = take_relayed(receiver)});
        deferred_elements_ += elements;
    }

    /// Send whatever the peers have since made room for. Walks a worklist that is empty whenever nothing is
    /// parked, so an uncongested run pays one branch per poll.
    void drain_deferred() {
        if (deferred_peers_.empty()) {
            return;
        }
        std::size_t kept = 0;
        for (std::size_t i = 0; i < deferred_peers_.size(); ++i) {
            PEID receiver = deferred_peers_[i];
            auto it = deferred_.find(receiver);
            if (it == deferred_.end()) {
                continue;
            }
            auto& packets = it->second;
            while (!packets.empty()) {
                auto const elements = packets.front().buffer.size();
                if (!flow_.has_credit(receiver, elements) || !queue_.has_send_capacity()) {
                    break;
                }
                auto const relayed = packets.front().relayed;
                auto receipt = queue_.post_message(std::move(packets.front().buffer), receiver);
                KASSERT(receipt.has_value(), "capacity was checked, so posting must succeed");
                flow_.note_sent(receiver, elements);
                if (relayed > 0) {
                    relayed_by_receipt_[*receipt] = relayed;
                }
                num_elements_flushed_ += elements;
                deferred_elements_ -= elements;
                packets.pop_front();
            }
            if (packets.empty()) {
                deferred_.erase(it);
            } else {
                deferred_peers_[kept++] = receiver;
            }
        }
        deferred_peers_.resize(kept);
    }

    /// Relayed elements accumulated for \p receiver's currently filling buffer, handed over to whatever
    /// takes ownership of that buffer (a send, or a deferred packet). The reserve they occupy is released
    /// when that send completes -- see reclaim_aggregation_buffer.
    std::size_t take_relayed(PEID receiver) {
        auto it = relayed_in_buffer_.find(receiver);
        if (it == relayed_in_buffer_.end()) {
            return 0;
        }
        auto const relayed = it->second;
        relayed_in_buffer_.erase(it);
        return relayed;
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
            auto const elements = buffer.message.size();
            // Anything posted while this is non-zero is a forward of relayed payload, not an application
            // send, and is charged to the relay reserve. A depth rather than a flag because handling can
            // nest: a post that blocks polls, and a poll can run another packet's handler.
            auto const source_class = link_class(source);
            bool const relays = source_class == LinkClass::to_proxy;
            flow_.set_link_class(source, source_class);
            relaying_depth_ += relays ? 1 : 0;
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
            relaying_depth_ -= relays ? 1 : 0;
            // Release the peer's window now that the packet has been handled, which may grant it more.
            flow_.note_admitted(source, elements);
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

    /// Return a buffer to the pool that was never sent. Deliberately NOT reclaim_aggregation_buffer with
    /// a dummy receipt: receipt 0 is a real id, so that would release some other send's relay reserve.
    void recycle_buffer(BufferContainer&& buffer) {
        num_buffer_recycles_++;
        buffer.resize(0);  // this does not reduce the capacity
        free_aggregation_buffers_.emplace_back(std::move(buffer));
    }

    auto reclaim_aggregation_buffer(std::size_t receipt, BufferContainer&& buffer) {
        // A completed send is the moment relayed payload stops occupying anything: it is no longer in an
        // aggregation buffer, a deferred queue, or MPI's hands. Releasing it any earlier would let a relay
        // grant room it does not have.
        num_buffer_reclaims_++;
        auto it = relayed_by_receipt_.find(receipt);
        if (it != relayed_by_receipt_.end()) {
            flow_.note_relay_released(it->second);
            relayed_by_receipt_.erase(it);
        }
        recycle_buffer(std::move(buffer));
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
        // THIS is the loop the async-grid stall lived in. It sits on the relay's path
        // (redirection_handler -> post_message_blocking -> handle_overflow -> here), and a relay spinning
        // here is a receive slot left disarmed, so the relay stops accepting from its row entirely. Under
        // flow control it is dead code: the flush below parks the packet instead of failing, so there is
        // nothing to wait for. The loop stays for the no-flow-control configuration, which is the A/B
        // control and still needs it.
        if (flow_.enabled()) {
            // Never block, and never poll from inside a message handler. Polling here is what nests
            // receive handling: every level holds a receive slot disarmed, and a relay with all its slots
            // disarmed is deaf to its row -- the defect itself, just reached from the other side. So the
            // application's post path keeps driving the transport (one non-blocking poll, no loop), and
            // the relay's post path does nothing but append. That is the AML rule: only a loop blocked on
            // a hop may drive that hop.
            if (relaying_depth_ == 0) {
                poll(std::forward<decltype(on_message)>(on_message));
                progress_hook();
            }
        } else {
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

    internal::FlowController flow_;
    /// Packets parked per destination, waiting for that destination to grant room. Per destination, not
    /// one shared FIFO: a shared queue lets a packet for a slow peer head-of-line block everything behind
    /// it regardless of where it is going, which is half of what made the old backlog unusable.
    std::unordered_map<PEID, std::deque<DeferredPacket>> deferred_;
    /// Destinations with something parked. Kept as a worklist so an uncongested poll costs one branch.
    std::vector<PEID> deferred_peers_;
    std::size_t deferred_elements_ = 0;
    /// Relayed payload merged into each destination's currently filling buffer, moved onto the packet when
    /// that buffer is flushed or parked.
    std::unordered_map<PEID, std::size_t> relayed_in_buffer_;
    /// Relayed payload per in-flight send, released when the send completes.
    std::unordered_map<std::size_t, std::size_t> relayed_by_receipt_;
    /// Nesting depth of relay-link packet handlers on the stack; see split_handler.
    std::size_t relaying_depth_ = 0;
    std::size_t relay_buffer_reserve_ = 0;
    std::size_t num_credit_deferrals_ = 0;
    std::size_t num_relay_buffer_stalls_ = 0;
    /// Counts calls to poll_throttled. Held here rather than in the underlying queue so that the
    /// throttle covers the flow controller and the deferred queues too; see poll_throttled.
    std::size_t poll_throttle_count_ = 0;
    /// Buffer-pool traffic. Only ever read from the stall tracer, and read as a TRIPLE: recycles
    /// exceeding acquires means buffers are entering the free list that the pool never handed out, which
    /// is possible here (see acquire_buffer's note on husks) and is why nothing may derive the number of
    /// buffers in use by subtracting one of these counters from another.
    std::size_t num_buffer_acquires_ = 0;
    std::size_t num_buffer_recycles_ = 0;
    std::size_t num_buffer_reclaims_ = 0;
    double stall_trace_interval_ = 0.0;
    std::chrono::steady_clock::time_point stall_trace_last_{};

    Merger merge;
    Splitter split;
    BufferCleaner pre_send_cleanup;
    size_t global_buffer_size_ = 0;

    FlushStrategy flush_strategy_;
};
}  // namespace briefkasten
