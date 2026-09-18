// Copyright (c) 2021-2026 Tim Niklas Uhl
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
#include <kassert/kassert.hpp>
#include "./buffered_queue.hpp"   // IWYU pragma: keep
#include "./detail/concepts.hpp"  // IWYU pragma: keep
#include "./detail/definitions.hpp"
#include "./detail/link_class.hpp"

namespace briefkasten {
template <typename T>
concept IndirectionScheme = requires(T scheme, MPI_Comm comm, PEID sender, PEID receiver) {
    { scheme.next_hop(sender, receiver) } -> std::same_as<PEID>;
    { scheme.should_redirect(sender, receiver) } -> std::same_as<bool>;
    // The grouping the scheme routes over: messages within a group go directly, cross-group messages go via one proxy
    // per group. `group_size` is the number of ranks per group, `num_groups` the number of groups. Together they bound
    // the fan-out and drive the buffering defaults (see fan_out below).
    { scheme.group_size() } -> std::convertible_to<std::size_t>;
    { scheme.num_groups() } -> std::convertible_to<std::size_t>;
    // Which obligation a link to `receiver` puts on its far end. Must be a property of the LINK alone --
    // not of the record travelling over it -- and both endpoints must agree on it; see detail/link_class.hpp.
    { scheme.link_class(receiver) } -> std::same_as<LinkClass>;
};

/// Routes every message over at most two hops of \p Indirector, using a SINGLE underlying queue.
///
/// ONE QUEUE, NOT TWO. Until 2026-09-18 this class ran two BufferedMessageQueues on two communicators, one
/// per hop, and a relay forwarded from inside the first hop's receive handler by blocking on the second
/// hop's send capacity. That shape is what produced the async-grid stall: a persistent receive is re-armed
/// only after its handler returns, so a relay blocked inside the handler stopped accepting from its row
/// entirely (4.09M fully deaf polls per iteration on rmat p=768). See notes/takeover_briefkasten_tokens.md.
///
/// Collapsing to one queue deletes, rather than fixes, most of the machinery that grew around that shape:
/// the second communicator, the fused sibling-hop termination (two sequential per-hop terminations
/// deadlock, so the second hop's counts had to be folded into the first hop's counting round via
/// `additional_counts`), the second hop's forced flushes, the `relay_drains_first_hop` progress hook, and
/// the frame chain `hop2 resolve_overflow_blocking -> progress_hook -> first_hop.poll ->
/// redirection_handler`. It also removes the one blocking loop that never took a progress hook at all
/// (MessageQueue::poll_until_no_outstanding_sends, which polled hop 1 only): with a single queue there is
/// no sibling left to starve.
///
/// What makes one queue safe -- and what made it unsafe before -- is that a relay now posts back into the
/// same queue it is being polled from. Every loop in BufferedMessageQueue that can poll therefore treats
/// `aggregation_buffers_` as mutable underneath it; see the re-entrancy notes there.
///
/// The relay still blocks on send capacity here. Not blocking is what the flow controller is for; this
/// class is what makes a single credit counter per peer sufficient, because with one queue a peer has
/// exactly one link and that link has exactly one class (see detail/link_class.hpp).
template <IndirectionScheme Indirector, typename BufferedQueueType>
class IndirectionAdapter {
private:
    using queue_type = BufferedQueueType;
    using MessageType = typename queue_type::message_type;

    queue_type queue_;

public:
    IndirectionAdapter(BufferedQueueType queue, Indirector indirector)
        : queue_(std::move(queue)), indirection_(std::move(indirector)) {
        // The queue was built and moved in by the caller, sized for a fan-out of p (every rank a potential
        // destination). Resize it to what indirection actually bounds the distinct destinations to: the
        // peers in our row (one proxy per column) plus the peers in our column (the relay hop's receivers),
        // which is O(sqrt p) for a square grid. Both hops now share this one queue, so this is a single
        // fan-out rather than the two per-hop ones the split version derived.
        auto cfg = apply_fan_out_defaults(queue_.config(), fan_out(indirection_));
        queue_.max_num_aggregation_buffers(cfg.max_num_aggregation_buffers.value());
        queue_.send_backlog_capacity(cfg.send_backlog_capacity.value());
        // One buffer and one class per peer. The queue caches this, so the scheme is consulted once per
        // peer rather than once per message.
        queue_.link_classifier([this](PEID peer) { return indirection_.link_class(peer); });
        // Flow control defaults ON here and only here. This is the class that relays, and relaying is what
        // makes a blocked send block a receive handler; a flat queue's handler is terminal and cannot
        // block. An explicit budget of 0 in the config turns it off, which is the A/B control.
        auto const window = queue_.config().credit_window_packets.value_or(DEFAULT_CREDIT_WINDOW_PACKETS);
        auto const parking =
            queue_.config().outbound_buffers_per_peer.value_or(DEFAULT_OUTBOUND_BUFFERS_PER_PEER);
        queue_.enable_flow_control(window, parking, fan_out(indirection_));
    }

    /// Enable the selective termination drain; see BufferedMessageQueue::flush_all_buffers_blocking.
    void selective_drain(bool enable) {
        queue_.selective_drain(enable);
    }

    auto& indirection_scheme() {
        return indirection_;
    }

    auto const& indirection_scheme() const {
        return indirection_;
    }

    bool post_message(InputMessageRange<MessageType> auto&& message,
                      PEID receiver,  // NOLINT(bugprone-*)
                      PEID envelope_sender,
                      PEID envelope_receiver,
                      int tag,
                      bool direct_send = false) {
        // `direct_send` is the relay path: the caller is a receive handler forwarding an already-proxied
        // message to its final destination, so the next hop is the destination itself.
        PEID next_hop = direct_send ? receiver : indirection_.next_hop(envelope_sender, envelope_receiver);
        return queue_.post_message(std::forward<decltype(message)>(message), next_hop, envelope_sender,
                                   envelope_receiver, tag);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(InputMessageRange<MessageType> auto&& message,
                      PEID receiver,
                      int tag = 0,
                      bool direct_send = false) {
        return post_message(std::forward<decltype(message)>(message), receiver, this->rank(), receiver, tag,
                            direct_send);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(typename queue_type::message_type message, PEID receiver, int tag = 0, bool direct_send = false) {
        return post_message(std::ranges::views::single(message), receiver, tag, direct_send);
    }

    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,  // NOLINT(bugprone-*)
                               PEID envelope_sender,
                               PEID envelope_receiver,
                               int tag,
                               MessageHandler<MessageType> auto&& on_message,
                               bool direct_send = false) {
        PEID next_hop = direct_send ? receiver : indirection_.next_hop(envelope_sender, envelope_receiver);
        // No progress hook: there is no sibling queue to drive. Every blocking loop inside the queue polls
        // THIS queue, which is now the only transport the whole two-hop system runs over.
        return queue_.post_message_blocking(std::forward<decltype(message)>(message), next_hop, envelope_sender,
                                            envelope_receiver, tag, redirection_handler(on_message));
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0,
                               bool direct_send = false) {
        return post_message_blocking(std::forward<decltype(message)>(message), receiver, this->rank(), receiver, tag,
                                     std::forward<decltype(on_message)>(on_message), direct_send);
    }

    bool post_message_blocking(MessageType message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0,
                               bool direct_send = false) {
        KASSERT(receiver < this->size());
        return post_message_blocking(std::ranges::views::single(message), receiver,
                                     std::forward<decltype(on_message)>(on_message), tag, direct_send);
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    auto poll(MessageHandler<typename queue_type::message_type> auto&& on_message)
        -> std::optional<std::pair<bool, bool>> {
        return queue_.poll(redirection_handler(on_message));
    }

    auto poll_throttled(MessageHandler<MessageType> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD)
        -> std::optional<std::pair<bool, bool>> {
        return queue_.poll_throttled(redirection_handler(on_message), poll_skip_threshold);
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    ///
    /// A single, ordinary termination over a single queue. Relayed payload that has been received but not
    /// yet forwarded is visible to the decision through MessageCounter::pending, exactly as locally
    /// originated payload is: the relay merged it into one of this queue's aggregation buffers, so it is
    /// counted by global_buffer_size_. That is what used to require the two hops' counts to be fused into
    /// one allreduce, and it is why the fuse can go -- there is only one count now, so the class of bug in
    /// notes/takeover_briefkasten_tokens.md section 5 (counted as received, not as sent, data still
    /// undelivered) has nowhere left to hide.
    [[nodiscard]] bool terminate(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return queue_.terminate(redirection_handler(on_message));
    }

    [[nodiscard]] bool terminate(MessageHandler<typename queue_type::message_type> auto&& on_message,
                                 std::invocable<> auto&& progress_hook) {
        return queue_.terminate(redirection_handler(on_message), std::forward<decltype(progress_hook)>(progress_hook));
    }

    /// \copydoc BufferedMessageQueue::terminate_throttled
    [[nodiscard]] bool terminate_throttled(MessageHandler<typename queue_type::message_type> auto&& on_message,
                                           std::size_t skip_threshold = 1) {
        return queue_.terminate_throttled(redirection_handler(on_message), skip_threshold);
    }

    void global_threshold_bytes(std::size_t new_threshold,
                                MessageHandler<typename queue_type::message_type> auto&& on_message) {
        queue_.global_threshold_bytes(new_threshold, redirection_handler(on_message));
    }

    void local_threshold_bytes(std::size_t new_threshold,
                               MessageHandler<typename queue_type::message_type> auto&& on_message) {
        queue_.local_threshold_bytes(new_threshold, redirection_handler(on_message));
    }

    [[nodiscard]] PEID rank() const {
        return queue_.rank();
    }

    [[nodiscard]] PEID size() const {
        return queue_.size();
    }

    /// if this mode is active, no incoming messages will cancel the termination process
    /// this allows using the queue as a somewhat async sparse-all-to-all
    void synchronous_mode(bool use_it = true) {
        queue_.synchronous_mode(use_it);
    }

    auto num_allocated_buffers() {
        return queue_.num_allocated_buffers();
    }

    [[nodiscard]] std::size_t num_termination_rounds() const {
        return queue_.num_termination_rounds();
    }

    /// The single underlying queue. Replaces the old first_hop_queue()/second_hop_queue() pair: callers
    /// that reported per-hop counters now report one set, because there is one transport.
    [[nodiscard]] queue_type const& queue() const {
        return queue_;
    }

    [[nodiscard]] queue_type& queue() {
        return queue_;
    }

private:
    /// Distinct destinations this queue aggregates to under indirection: the proxies we reach on the first
    /// hop (one per column, plus the same-column receivers we reach directly) and the receivers we forward
    /// to on the second hop (the members of our column). Upper-bounded by their sum, which is O(sqrt p) for
    /// a square grid -- this is what keeps startup overhead (live MPI partners) tractable at scale.
    static std::size_t fan_out(Indirector const& indirector) {
        return static_cast<std::size_t>(indirector.group_size()) + static_cast<std::size_t>(indirector.num_groups());
    }

    auto redirection_handler(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return [&](Envelope<typename queue_type::message_type> auto envelope) {
            KASSERT(envelope.receiver < this->size());
            bool should_redirect = indirection_.should_redirect(envelope.sender, envelope.receiver);
            if (should_redirect) {
                post_message_blocking(std::move(envelope.message), envelope.receiver, envelope.sender,
                                      envelope.receiver, envelope.tag, on_message,
                                      /* direct_send = */ true);
            } else {
                KASSERT(envelope.receiver == this->rank());
                on_message(std::move(envelope));
            }
        };
    }
    Indirector indirection_;
};

}  // namespace briefkasten
