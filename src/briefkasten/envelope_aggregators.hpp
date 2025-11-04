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

#include <algorithm>
#include <array>
#include <concepts>
#include <ranges>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <kamping/mpi_datatype.hpp>
#include <kassert/kassert.hpp>

#include "./detail/concepts.hpp"
#include "./detail/view_adaptors.hpp"

namespace briefkasten::aggregation {

// type traits

template <typename From, typename To>
struct all_members_convertible_to : std::false_type {};

template <typename First, typename Second, typename To>
struct all_members_convertible_to<std::pair<First, Second>, To>
    : std::conjunction<std::is_convertible<First, To>, std::is_convertible<Second, To>> {};

template <typename... Ts, typename To>
struct all_members_convertible_to<std::tuple<Ts...>, To> : std::conjunction<std::is_convertible<Ts, To>...> {};

template <typename From, typename To>
concept is_serializable_to = std::is_convertible_v<From, To> || all_members_convertible_to<From, To>::value;

template <typename EnvType, typename BufferContainer>
concept SerializableEnvelope =
    MPIBuffer<BufferContainer> && Envelope<EnvType> &&
    is_serializable_to<typename EnvType::message_value_type, std::ranges::range_value_t<BufferContainer>> &&
    std::is_convertible_v<PEID, std::ranges::range_value_t<BufferContainer>> &&
    std::is_convertible_v<std::size_t, std::ranges::range_value_t<BufferContainer>>;

namespace detail {

template <typename T>
constexpr std::size_t element_cardinality() {
    if constexpr (requires { std::tuple_size<T>::value; }) {
        return std::tuple_size<T>::value;
    } else {
        return 1;
    }
}

template <typename BufferType, typename ValueType>
auto convert_to_buffer(ValueType const& value) {
    constexpr std::size_t elem_size = element_cardinality<ValueType>();
    if constexpr (elem_size > 1) {
        std::array<BufferType, elem_size> buf;
        kamping::internal::for_each_tuple_field(
            value, [&buf](auto const& val, std::size_t idx) { buf[idx] = static_cast<BufferType>(val); });
        return buf;
    } else {
        return static_cast<BufferType>(value);
    }
}

template <typename ElementType, typename Range, std::size_t... Is>
auto chunk_to_element_impl(Range&& chunk, std::index_sequence<Is...> /* sequence */) {
    KASSERT(chunk.size() == sizeof...(Is), "Chunk size does not match element size");
    if constexpr (sizeof...(Is) > 1) {
        return ElementType{static_cast<typename std::tuple_element<Is, ElementType>::type>(chunk[Is])...};
    } else {
        return static_cast<ElementType>(chunk[0]);
    }
}

template <typename ElementType, typename Range>
ElementType reconstruct_from_chunk(Range&& chunk) {
    return chunk_to_element_impl<ElementType>(std::forward<Range>(chunk),
                                              std::make_index_sequence<element_cardinality<ElementType>()>{});
}

}  // namespace detail

enum class EnvelopeMetadataField { size, tag, sender, receiver };

template <EnvelopeMetadataField... meta>
struct EnvelopeMetadata {
    static constexpr std::array<EnvelopeMetadataField, sizeof...(meta)> values = {meta...};
    static_assert(
        [] {
            auto values_copy = values;
            std::ranges::sort(values_copy);
            auto it = std::ranges::adjacent_find(values_copy);
            return it == values_copy.end();
        }(),
        "Duplicate metadata fields are not allowed");
    static constexpr std::size_t size = sizeof...(meta);
    static constexpr bool contains(EnvelopeMetadataField field) {
        auto it = std::ranges::find(values, field);
        return it != values.end();
    }
};

template <typename T>
concept is_integral_constant = requires {
    typename T::value_type;
    { T::value } -> std::convertible_to<typename T::value_type>;
};
template <typename T, typename U>
concept is_integral_constant_of_type = is_integral_constant<T> && std::same_as<typename T::value_type, U>;

template <typename metadata = EnvelopeMetadata<EnvelopeMetadataField::size, EnvelopeMetadataField::receiver>,
          typename message_size = void>
struct EnvelopeSerializationMerger {
    static_assert(
        metadata::contains(EnvelopeMetadataField::size) || (is_integral_constant_of_type<message_size, std::size_t>),
        "When not including size in the metadata, message_size must be an integral constant of type std::size_t "
        "containing the fixed message size.");

    template <MPIBuffer BufferContainer,
              SerializableEnvelope<BufferContainer> EnvType>  // requires that both the message value type and
    void operator()(BufferContainer& buffer, PEID /* buffer_destination */, PEID /* my_rank */, EnvType envelope) {
        using env_type = EnvType::message_value_type;
        using buffer_type = std::ranges::range_value_t<BufferContainer>;

        constexpr std::size_t elem_size = detail::element_cardinality<env_type>();
        const std::size_t message_buffer_elements = envelope.message.size() * elem_size;

        std::vector<buffer_type> combined(message_buffer_elements +
                                          metadata::size);  // extra space for receiver and size
        std::size_t meta_index = 0;
        if constexpr (metadata::contains(EnvelopeMetadataField::size)) {
            combined[meta_index++] =
                static_cast<buffer_type>(envelope.message.size() * elem_size) + metadata::size -
                1;  // when chunking by size, we need to include the metadata fields except size itself
        }
        if constexpr (metadata::contains(EnvelopeMetadataField::sender)) {
            combined[meta_index++] = static_cast<buffer_type>(envelope.sender);
        }
        if constexpr (metadata::contains(EnvelopeMetadataField::receiver)) {
            combined[meta_index++] = static_cast<buffer_type>(envelope.receiver);
        }
        if constexpr (metadata::contains(EnvelopeMetadataField::tag)) {
            combined[meta_index++] = static_cast<buffer_type>(envelope.tag);
        }

        // Copy the message into the combined vector, starting from index 2
        // and transform the message values to the buffer type.
        auto converted_message = envelope.message | std::views::transform([](env_type const& value) {
                                     return detail::convert_to_buffer<buffer_type>(value);
                                 });
        if constexpr (elem_size > 1) {
            std::ranges::copy(converted_message | std::views::join, combined.begin() + meta_index);
        } else {
            std::ranges::copy(converted_message, combined.begin() + meta_index);
        }
        buffer.insert(buffer.end(), combined.begin(), combined.end());
    }

    template <MPIBuffer BufferContainer, SerializableEnvelope<BufferContainer> EnvType>
    size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                    PEID /* buffer_destination */,
                                    PEID /* my_rank */,
                                    EnvType const& envelope) const {
        constexpr std::size_t elem_size = detail::element_cardinality<typename EnvType::message_value_type>();
        return buffer.size() + (envelope.message.size() * elem_size) + metadata::size;  // 2 for receiver and size
    }
};

static_assert(Merger<EnvelopeSerializationMerger<>, int, std::vector<int>>);
static_assert(Merger<EnvelopeSerializationMerger<>, std::pair<int, int>, std::vector<int>>);

template <typename MessageType,
          typename metadata = EnvelopeMetadata<EnvelopeMetadataField::size, EnvelopeMetadataField::receiver>,
          typename message_size = void>
struct EnvelopeSerializationSplitter {
    static_assert(
        metadata::contains(EnvelopeMetadataField::size) || (is_integral_constant_of_type<message_size, std::size_t>),
        "When not including size in the metadata, message_size must be an integral constant of type std::size_t "
        "containing the fixed message size.");

    template <MPIBuffer BufferContainer>
    auto operator()(BufferContainer const& buffer, PEID buffer_origin, PEID my_rank) const {
        auto chunk = [] {
            if constexpr (metadata::contains(EnvelopeMetadataField::size)) {
                // size is always the first field
                return chunk_by_embedded_size(0);
            } else {
                return std::views::chunk((message_size::value * detail::element_cardinality<MessageType>()) +
                                         metadata::size);
            }
        }();
        auto split_to_envelope =
            chunk | std::views::transform([&](auto chunk) {
                PEID sender = buffer_origin;
                PEID receiver = my_rank;
                PEID tag = 0;
                std::size_t meta_index = metadata::contains(EnvelopeMetadataField::size) ? 1 : 0;  // start after size
                if constexpr (metadata::contains(EnvelopeMetadataField::sender)) {
                    sender = static_cast<PEID>(chunk[meta_index++]);
                }
                if constexpr (metadata::contains(EnvelopeMetadataField::receiver)) {
                    receiver = static_cast<PEID>(chunk[meta_index++]);
                }
                if constexpr (metadata::contains(EnvelopeMetadataField::tag)) {
                    tag = static_cast<PEID>(chunk[meta_index++]);
                }

                auto message_data = chunk | std::views::drop(meta_index);
                if constexpr (detail::element_cardinality<MessageType>() > 1) {
                    auto message = message_data | std::views::chunk(detail::element_cardinality<MessageType>()) |
                                   std::views::transform([&](auto chunk) {
                                       return detail::reconstruct_from_chunk<MessageType>(std::move(chunk));
                                   });
                    return MessageEnvelope{std::move(message), sender, receiver, tag};
                } else {
                    auto message = message_data | std::views::transform(
                                                      [](auto const& elem) { return static_cast<MessageType>(elem); });
                    return MessageEnvelope{std::move(message), sender, receiver, tag};
                }
            });
        return buffer | split_to_envelope;
    }
};

}  // namespace briefkasten::aggregation
