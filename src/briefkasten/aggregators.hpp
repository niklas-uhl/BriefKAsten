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
#include <iterator>
#include <kamping/mpi_datatype.hpp>
#include <ranges>
#include <span>
#include <tuple>
#include <type_traits>
#include <print>
#include "./detail/concepts.hpp"
#include "./detail/view_adaptors.hpp"

namespace briefkasten::aggregation {

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

struct EnvelopeSerializationMerger {
    template <MPIBuffer BufferContainer,
              SerializableEnvelope<BufferContainer> EnvType>  // requires that both the message value type and
    void operator()(BufferContainer& buffer, PEID /* buffer_destination */, PEID /* my_rank */, EnvType envelope) {
        using env_type = EnvType::message_value_type;
        using buffer_type = std::ranges::range_value_t<BufferContainer>;

        constexpr std::size_t elem_size = [&] {
            if constexpr (requires { std::tuple_size<env_type>::value; }) {
                return std::tuple_size<env_type>::value;
            } else {
                return 1;
            }
        }();
        std::vector<buffer_type> combined((envelope.message.size() * elem_size) + 2);  // extra space for receiver and size
        combined[0] = static_cast<buffer_type>(envelope.receiver);
        combined[1] = static_cast<buffer_type>(envelope.message.size() * elem_size);
        // Copy the message into the combined vector, starting from index 2
        // and transform the message values to the buffer type.
        auto converted_message =
            envelope.message | std::views::transform([](env_type const& value) /* -> buffer_type */ {
                if constexpr (elem_size > 1) {
                    std::array<buffer_type, elem_size> buf;
                    std::size_t i = 0;
                    auto fn = [&](auto const& val, auto /* idx */) {
                        buf[i++] = static_cast<buffer_type>(val);
                    };
		    kamping::internal::for_each_tuple_field(value, fn);
                    // std::apply(fn, value);
                    return buf;
                } else {
                    return static_cast<buffer_type>(value);
                }
            });
	// std::println("converted_message {}", converted_message);
        if constexpr (elem_size > 1) {
            std::ranges::copy(converted_message | std::views::join, combined.begin() + 2);
        } else {
            std::ranges::copy(converted_message, combined.begin() + 2);
        }
	/* std::println("msg:{}, combined: {}", envelope.message, combined); */
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

static_assert(Merger<EnvelopeSerializationMerger, int, std::vector<int>>);
static_assert(Merger<EnvelopeSerializationMerger, std::pair<int, int>, std::vector<int>>);

template <typename MessageType>
struct EnvelopeSerializationSplitter {
    template <MPIBuffer BufferContainer>
    auto operator()(BufferContainer const& buffer, PEID /* buffer_origin */, PEID /* my_rank */) const {
      constexpr std::size_t elem_size = [&] {
	if constexpr (requires { std::tuple_size<MessageType>::value; }) {
	  return std::tuple_size<MessageType>::value;
	} else {
	  return 1;
	}
      }();
      return buffer | chunk_by_embedded_size(1) | std::views::transform([&](auto chunk) {
	PEID receiver = static_cast<PEID>(chunk[0]);
	auto message_data = chunk | std::views::drop(2);  /* | std::views::transform([](auto& /\* elem *\/) { */
	/*   /\* return MessageType{}; *\/ */
	/*   return static_cast<MessageType>(elem); */
        /* }); */
	if constexpr (elem_size > 1) {
	  auto message = message_data | std::views::chunk(elem_size) | std::views::transform([&](auto tup) {
	      std::array<typename std::tuple_element<0, MessageType>::type, elem_size> arr{};
	      std::size_t i = 0;
	      for (auto const& val : tup) {
		arr[i++] = val;
	      }
	      return std::apply([](auto const&... vals) { return MessageType{vals...}; }, arr);
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

struct NoOpCleaner {
    template <typename BufferContainer>
    void operator()(BufferContainer& /* buffer */, PEID /* buffer_destination */) const {}
};
static_assert(BufferCleaner<NoOpCleaner, std::vector<int>>);

}  // namespace briefkasten::aggregation
