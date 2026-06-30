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

#include <concepts>
#include <cstdint>
#include <iterator>
#include <limits>
#include <ranges>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include "./detail/concepts.hpp"
#include "./detail/view_adaptors.hpp"

namespace briefkasten::aggregation {

/// @brief Concept for fixed-size tuple-like message types (e.g. \c std::tuple, \c std::pair)
/// whose elements convert to/from the buffer's scalar value type.
template <typename T>
concept TupleLike = requires { std::tuple_size_v<std::remove_cvref_t<T>>; } &&
                    (std::tuple_size_v<std::remove_cvref_t<T>> > 0);

/// @brief True iff every value of integer type \p From is representable in integer type \p To.
/// Equivalent to "casting From -> To never narrows": \p To must be able to hold negative values when
/// \p From is signed, and have at least as many value bits.
template <typename From, typename To>
concept LosslesslyRepresentable = std::integral<From> && std::integral<To> &&
                                  (std::is_signed_v<To> || std::is_unsigned_v<From>) &&
                                  (std::numeric_limits<To>::digits >= std::numeric_limits<From>::digits);

/// @brief True unless converting integer \p From to integer \p To would narrow. Non-integer operands are
/// deliberately accepted, leaving floating-point (or otherwise convertible) payloads to the aggregator's own
/// constraints; only the silent integer-narrowing footgun is rejected.
template <typename From, typename To>
inline constexpr bool no_integer_narrowing_v =
    !(std::integral<From> && std::integral<To>) || LosslesslyRepresentable<From, To>;

/// @brief True iff every *non-negative* value of integer type \p From is representable in integer type \p To,
/// i.e. \p To has at least as many magnitude (value) bits. Unlike \ref LosslesslyRepresentable this ignores
/// signedness, so a signed source fits an unsigned target. Used for framing quantities that are never
/// negative (a \ref PEID rank), which therefore round-trip through an unsigned buffer.
template <typename From, typename To>
concept MagnitudeRepresentable = std::integral<From> && std::integral<To> &&
                                 (std::numeric_limits<To>::digits >= std::numeric_limits<From>::digits);

/// @brief True iff a (non-negative) \ref PEID rank round-trips through buffer element type \p To. Lenient for
/// non-integer buffers, which are governed by the aggregator's own convertibility constraints.
template <typename To>
inline constexpr bool buffer_holds_rank_v = !std::integral<To> || MagnitudeRepresentable<PEID, To>;

/// @brief True iff the embedded receiver (a \ref PEID) and every field of tuple-like \p Tuple fit into the
/// scalar buffer element type \p BufferType. Fields must round-trip losslessly; the non-negative receiver only
/// needs its magnitude represented (so an unsigned buffer is fine for unsigned-integer messages). Used to
/// reject too-narrow buffers at compile time instead of silently truncating in \ref TupleMerger / \ref TupleSplitter.
template <TupleLike Tuple, typename BufferType>
inline constexpr bool tuple_fits_buffer_v =
    buffer_holds_rank_v<BufferType> && []<std::size_t... I>(std::index_sequence<I...>) {
        return (LosslesslyRepresentable<std::tuple_element_t<I, std::remove_cvref_t<Tuple>>, BufferType> && ...);
    }(std::make_index_sequence<std::tuple_size_v<std::remove_cvref_t<Tuple>>>{});

// Unsigned messages need an unsigned buffer; the non-negative receiver rank still round-trips through it.
static_assert(tuple_fits_buffer_v<std::tuple<std::uint64_t, std::uint64_t>, std::uint64_t>);
// ...but a signed buffer cannot hold the top half of an unsigned field's range.
static_assert(!tuple_fits_buffer_v<std::tuple<std::uint64_t>, std::int64_t>);
// A narrower buffer that cannot hold the rank magnitude is rejected even when the field would fit.
static_assert(!tuple_fits_buffer_v<std::tuple<std::int16_t>, std::int16_t>);

struct AppendMerger {
    template <MPIBuffer BufferContainer>
    void operator()(BufferContainer& buffer,
                    PEID /* buffer_destination */,
                    PEID /* my_rank */,
                    Envelope<std::ranges::range_value_t<BufferContainer>> auto envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
    }

    template <typename MessageContainer, typename BufferContainer>
    [[nodiscard]] size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                                  PEID /* buffer_destination */,
                                                  PEID /* my_rank */,
                                                  MessageEnvelope<MessageContainer> const& envelope) const {
        return buffer.size() + envelope.message.size();
    }
};
static_assert(Merger<AppendMerger, int, std::vector<int>>);
static_assert(EstimatingMerger<AppendMerger, int, std::vector<int>>);

struct NoSplitter {
    auto operator()(MPIBuffer auto const& buffer, PEID buffer_origin, PEID my_rank) const {
        MessageEnvelope envelope{std::ranges::ref_view{buffer}, buffer_origin, my_rank, 0};
        return std::ranges::single_view{std::move(envelope)};
    }
};
static_assert(Splitter<NoSplitter, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelMerger {
    SentinelMerger(BufferType sentinel) : sentinel_(sentinel) {}

    void operator()(MPIBuffer<BufferType> auto& buffer,
                    PEID /* buffer_destination */,
                    PEID /* my_rank */,
                    Envelope<BufferType> auto envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
        buffer.push_back(sentinel_);
    }

    [[nodiscard]] size_t estimate_new_buffer_size(MPIBuffer<BufferType> auto const& buffer,
                                                  PEID /* buffer_destination */,
                                                  PEID /* my_rank */,
                                                  Envelope<BufferType> auto const& envelope) const {
        return buffer.size() + envelope.message.size() + 1;
    };
    BufferType sentinel_;
};
static_assert(Merger<SentinelMerger<int>, int, std::vector<int>>);
static_assert(EstimatingMerger<SentinelMerger<int>, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelSplitter {
    SentinelSplitter(BufferType sentinel) : sentinel_(sentinel) {}

    // NOLINTNEXTLINE(*-easily-swappable-parameters)
    auto operator()(MPIBuffer<BufferType> auto const& buffer, PEID buffer_origin, PEID my_rank) const {
        return buffer | std::views::take(buffer.size() - 1) |  // drop the last sentinel
               std::views::split(sentinel_) |
               std::views::transform([&, buffer_origin = buffer_origin, my_rank = my_rank](auto range) {
#ifdef MESSAGE_QUEUE_SPLIT_VIEW_IS_LAZY
                   auto size = std::ranges::distance(range);
                   auto sized_range = std::views::counted(range.begin(), size);
#else
                   auto sized_range = std::move(range);
#endif
                   static_assert(std::ranges::borrowed_range<decltype(sized_range)>);
                   return MessageEnvelope{std::move(sized_range), buffer_origin, my_rank, 0};
               });
    }
    BufferType sentinel_;
};

static_assert(Splitter<SentinelSplitter<int>, int, std::vector<int>>);

template <typename EnvType, typename BufferContainer>
concept SerializableEnvelope =
    MPIBuffer<BufferContainer> && Envelope<EnvType> &&
    std::is_convertible_v<typename EnvType::message_value_type, std::ranges::range_value_t<BufferContainer>> &&
    std::is_convertible_v<PEID, std::ranges::range_value_t<BufferContainer>> &&
    std::is_convertible_v<std::size_t, std::ranges::range_value_t<BufferContainer>>;

struct EnvelopeSerializationMerger {
    template <MPIBuffer BufferContainer,
              SerializableEnvelope<BufferContainer> EnvType>  // requires that both the message value type and
    void operator()(BufferContainer& buffer, PEID /* buffer_destination */, PEID /* my_rank */, EnvType envelope) {
        using env_type = EnvType::message_value_type;
        using buffer_type = std::ranges::range_value_t<BufferContainer>;
        static_assert(no_integer_narrowing_v<env_type, buffer_type> && buffer_holds_rank_v<buffer_type>,
                      "EnvelopeSerializationMerger: a message element (or the receiver PEID) would be narrowed by the "
                      "buffer element type. Widen the scalar buffer, e.g. with_buffer_type<int64_t>() (or an unsigned "
                      "buffer for unsigned messages).");
        std::vector<buffer_type> combined(envelope.message.size() + 2);  // extra space for receiver and size
        combined[0] = static_cast<buffer_type>(envelope.receiver);
        combined[1] = static_cast<buffer_type>(envelope.message.size());
        // Copy the message into the combined vector, starting from index 2
        // and transform the message values to the buffer type.
        auto converted_message = envelope.message | std::views::transform([](env_type const& value) -> buffer_type {
                                     return static_cast<buffer_type>(value);
                                 });
        std::ranges::copy(converted_message, combined.begin() + 2);
        buffer.insert(buffer.end(), combined.begin(), combined.end());
    }
    template <MPIBuffer BufferContainer, SerializableEnvelope<BufferContainer> EnvType>
    size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                    PEID /* buffer_destination */,
                                    PEID /* my_rank */,
                                    EnvType const& envelope) const {
        return buffer.size() + envelope.message.size() + 2;  // 2 for receiver and size
    }
};

template <typename MessageType>
struct EnvelopeSerializationSplitter {
    template <MPIBuffer BufferContainer>
    auto operator()(BufferContainer const& buffer, PEID /* buffer_origin */, PEID /* my_rank */) const {
        using buffer_type = std::ranges::range_value_t<BufferContainer>;
        static_assert(no_integer_narrowing_v<MessageType, buffer_type> && buffer_holds_rank_v<buffer_type>,
                      "EnvelopeSerializationSplitter: a message element (or the receiver PEID) would be narrowed by the "
                      "buffer element type. Widen the scalar buffer, e.g. with_buffer_type<int64_t>() (or an unsigned "
                      "buffer for unsigned messages).");
        return buffer | chunk_by_embedded_size(1) | std::views::transform([](auto chunk) {
                   PEID receiver = static_cast<PEID>(chunk[0]);
                   auto message = chunk | std::views::drop(2) |
                                  std::views::transform([](auto& elem) { return static_cast<MessageType>(elem); });
                   return MessageEnvelope{std::move(message), 0, receiver, 0};
               });
    }
};

/// @brief Merger that serializes fixed-size tuple-like messages (\c std::tuple, \c std::pair, ...)
/// into a flat scalar buffer, prepending each message's final receiver as the first slot of its
/// fixed-width record:
///   [ receiver, field_0, field_1, ..., field_{N-1} ]
/// Because the receiver survives serialization, this merger (paired with \ref TupleSplitter) supports
/// routing indirection: an intermediate hop can recover the final destination after splitting and
/// forward accordingly. Pair it with a scalar buffer type (e.g. \c with_buffer_type<int64_t>()) that
/// every tuple field converts to.
struct TupleMerger {
    template <MPIBuffer BufferContainer, Envelope EnvType>
        requires TupleLike<typename EnvType::message_value_type>
    void operator()(BufferContainer& buffer, PEID /* buffer_destination */, PEID /* my_rank */, EnvType envelope) const {
        using buffer_type = std::ranges::range_value_t<BufferContainer>;
        static_assert(tuple_fits_buffer_v<typename EnvType::message_value_type, buffer_type>,
                      "TupleMerger: a message field (or the receiver PEID) does not fit losslessly into the buffer "
                      "element type. Widen the scalar buffer, e.g. with_buffer_type<int64_t>().");
        for (auto const& message : envelope.message) {
            buffer.push_back(static_cast<buffer_type>(envelope.receiver));
            std::apply([&](auto const&... fields) { (buffer.push_back(static_cast<buffer_type>(fields)), ...); },
                       message);
        }
    }

    template <MPIBuffer BufferContainer, Envelope EnvType>
        requires TupleLike<typename EnvType::message_value_type>
    [[nodiscard]] std::size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                                       PEID /* buffer_destination */,
                                                       PEID /* my_rank */,
                                                       EnvType const& envelope) const {
        constexpr std::size_t record_width = 1 + std::tuple_size_v<typename EnvType::message_value_type>;
        return buffer.size() + (envelope.message.size() * record_width);
    }
};
static_assert(Merger<TupleMerger, std::pair<int, int>, std::vector<int>>);
static_assert(EstimatingMerger<TupleMerger, std::pair<int, int>, std::vector<int>>);
static_assert(Merger<TupleMerger, std::tuple<int, int, int>, std::vector<int>>);
static_assert(EstimatingMerger<TupleMerger, std::tuple<int, int, int>, std::vector<int>>);

/// @brief Splitter counterpart to \ref TupleMerger. Reconstructs fixed-size tuple-like messages from a
/// flat scalar buffer and reports each message's embedded receiver in the envelope, so the indirection
/// adapter can re-route messages that have not yet reached their final destination.
template <TupleLike Message>
struct TupleSplitter {
    static constexpr std::size_t arity = std::tuple_size_v<Message>;
    static constexpr std::size_t record_width = 1 + arity;

    auto operator()(MPIBuffer auto const& buffer, PEID /* buffer_origin */, PEID /* my_rank */) const {
        using buffer_type = std::ranges::range_value_t<std::remove_cvref_t<decltype(buffer)>>;
        static_assert(tuple_fits_buffer_v<Message, buffer_type>,
                      "TupleSplitter: a message field (or the receiver PEID) does not fit losslessly into the buffer "
                      "element type. Widen the scalar buffer, e.g. with_buffer_type<int64_t>().");
        auto first = std::ranges::begin(buffer);
        std::size_t const num_records = std::ranges::size(buffer) / record_width;
        return std::views::iota(std::size_t{0}, num_records) | std::views::transform([first](std::size_t record) {
                   auto const base = static_cast<std::ptrdiff_t>(record * record_width);
                   auto receiver = static_cast<PEID>(first[base]);
                   Message message = [&]<std::size_t... I>(std::index_sequence<I...>) {
                       return Message{static_cast<std::tuple_element_t<I, Message>>(
                           first[base + 1 + static_cast<std::ptrdiff_t>(I)])...};
                   }(std::make_index_sequence<arity>{});
                   return MessageEnvelope{std::ranges::single_view{std::move(message)}, PEID{0}, receiver, 0};
               });
    }
};
static_assert(Splitter<TupleSplitter<std::pair<int, int>>, std::pair<int, int>, std::vector<int>>);
static_assert(Splitter<TupleSplitter<std::tuple<int, int, int>>, std::tuple<int, int, int>, std::vector<int>>);

struct NoOpCleaner {
    template <typename BufferContainer>
    void operator()(BufferContainer& /* buffer */, PEID /* buffer_destination */) const {}
};
static_assert(BufferCleaner<NoOpCleaner, std::vector<int>>);

}  // namespace briefkasten::aggregation
