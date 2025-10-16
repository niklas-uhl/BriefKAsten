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

#include <array>
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

struct EnvelopeSerializationMerger {
    template <MPIBuffer BufferContainer,
              SerializableEnvelope<BufferContainer> EnvType>  // requires that both the message value type and
    void operator()(BufferContainer& buffer, PEID /* buffer_destination */, PEID /* my_rank */, EnvType envelope) {
        using env_type = EnvType::message_value_type;
        using buffer_type = std::ranges::range_value_t<BufferContainer>;

        constexpr std::size_t elem_size = detail::element_cardinality<env_type>();
        const std::size_t message_buffer_elements = envelope.message.size() * elem_size;

        std::vector<buffer_type> combined(message_buffer_elements + 2);  // extra space for receiver and size
        combined[0] = static_cast<buffer_type>(envelope.receiver);
        combined[1] = static_cast<buffer_type>(envelope.message.size() * elem_size);

        // Copy the message into the combined vector, starting from index 2
        // and transform the message values to the buffer type.
        auto converted_message = envelope.message | std::views::transform([](env_type const& value) {
                                     return detail::convert_to_buffer<buffer_type>(value);
                                 });
        if constexpr (elem_size > 1) {
            std::ranges::copy(converted_message | std::views::join, combined.begin() + 2);
        } else {
            std::ranges::copy(converted_message, combined.begin() + 2);
        }
        buffer.insert(buffer.end(), combined.begin(), combined.end());
    }
    template <MPIBuffer BufferContainer, SerializableEnvelope<BufferContainer> EnvType>
    size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                    PEID /* buffer_destination */,
                                    PEID /* my_rank */,
                                    EnvType const& envelope) const {
        constexpr std::size_t elem_size = detail::element_cardinality<typename EnvType::message_value_type>();
        return buffer.size() + (envelope.message.size() * elem_size) + 2;  // 2 for receiver and size
    }
};

static_assert(Merger<EnvelopeSerializationMerger, int, std::vector<int>>);
static_assert(Merger<EnvelopeSerializationMerger, std::pair<int, int>, std::vector<int>>);

template <typename MessageType>
struct EnvelopeSerializationSplitter {
    template <MPIBuffer BufferContainer>
    auto operator()(BufferContainer const& buffer, PEID /* buffer_origin */, PEID /* my_rank */) const {
        return buffer | chunk_by_embedded_size(1) | std::views::transform([&](auto chunk) {
                   PEID receiver = static_cast<PEID>(chunk[0]);
                   auto message_data = chunk | std::views::drop(2);
                   if constexpr (detail::element_cardinality<MessageType>() > 1) {
                       auto message = message_data | std::views::chunk(detail::element_cardinality<MessageType>()) |
                                      std::views::transform([&](auto chunk) {
                                          return detail::reconstruct_from_chunk<MessageType>(std::move(chunk));
                                      });
                       return MessageEnvelope{std::move(message), 0, receiver, 0};
                   } else {
                       auto message = message_data | std::views::transform([](auto const& elem) {
                                          return static_cast<MessageType>(elem);
                                      });
                       return MessageEnvelope{std::move(message), 0, receiver, 0};
                   }
               });
    }
};

}  // namespace briefkasten::aggregation
