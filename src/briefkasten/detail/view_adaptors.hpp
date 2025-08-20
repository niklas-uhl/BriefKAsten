#pragma once

#include <iterator>
#include <ranges>
#include <span>

namespace briefkasten {

// Simple iterator that chunks data based on embedded size field
template <std::forward_iterator UnderlyingIteratorType>
class chunk_iterator {
public:
    // Iterator traits
    using value_type = std::ranges::subrange<UnderlyingIteratorType>;
    using difference_type = std::ptrdiff_t;
    using reference = value_type;
    using pointer = void;
    using iterator_category = std::input_iterator_tag;
    using iterator_concept = std::forward_iterator_tag;

private:
    UnderlyingIteratorType current_;
    UnderlyingIteratorType end_;
    std::size_t size_offset_;

public:
    // Default constructor
    chunk_iterator() : current_(), end_(), size_offset_(0) {}

    // Constructor
    chunk_iterator(UnderlyingIteratorType current, UnderlyingIteratorType end, std::size_t size_offset)
        : current_(current), end_(end), size_offset_(size_offset) {}

    // Dereference
    value_type operator*() const {
        if (current_ >= end_ || current_ + size_offset_ >= end_) {
            return {};
        }

        auto chunk_size = static_cast<std::size_t>(current_[size_offset_]);
        UnderlyingIteratorType chunk_end = current_ + size_offset_ + 1 + chunk_size;

        if (chunk_end > end_) {
            chunk_end = end_;
        }

        return {current_, chunk_end};
    }

    // Pre-increment
    chunk_iterator& operator++() {
        if (current_ >= end_ || current_ + size_offset_ >= end_) {
            current_ = end_;
            return *this;
        }

        auto chunk_size = static_cast<std::size_t>(current_[size_offset_]);
        current_ += size_offset_ + 1 + chunk_size;

        if (current_ > end_) {
            current_ = end_;
        }

        return *this;
    }

    // Post-increment
    chunk_iterator operator++(int) {
        auto tmp = *this;
        ++*this;
        return tmp;
    }

    // Equality
    bool operator==(const chunk_iterator& other) const {
        return current_ == other.current_;
    }

    bool operator!=(const chunk_iterator& other) const {
        return !(*this == other);
    }
};

// Range view that wraps our iterator
template <std::ranges::contiguous_range Range>
class chunk_by_embedded_size_view : public std::ranges::view_interface<chunk_by_embedded_size_view<Range>> {
private:
    Range range_;
    std::size_t size_offset_ = 0;

public:
    using element_type = std::ranges::range_value_t<Range>;
    using iterator = chunk_iterator<std::ranges::iterator_t<Range>>;
    using const_iterator = chunk_iterator<decltype(std::ranges::cbegin(
        std::declval<Range&>())) /* std::ranges::const_iterator_t<Range> */>;

    chunk_by_embedded_size_view() = default;

    chunk_by_embedded_size_view(Range range, std::size_t size_offset)
        : range_(std::forward<Range>(range)), size_offset_(size_offset) {}

    iterator begin() {
        return iterator(std::ranges::begin(range_), std::ranges::end(range_), size_offset_);
    }

    iterator end() {
        return iterator(std::ranges::end(range_), std::ranges::end(range_), size_offset_);
    }

    [[nodiscard]] const_iterator begin() const {
        return const_iterator(std::ranges::cbegin(range_), std::ranges::cend(range_), size_offset_);
    }

    [[nodiscard]] const_iterator end() const {
        return const_iterator(std::ranges::cend(range_), std::ranges::cend(range_), size_offset_);
    }

    [[nodiscard]] bool empty() const {
        return std::ranges::empty(range_);
    }
};

// Range adaptor closure for piping
struct chunk_by_embedded_size_adaptor : std::ranges::range_adaptor_closure<chunk_by_embedded_size_adaptor> {
    std::size_t size_offset_;

    explicit chunk_by_embedded_size_adaptor(std::size_t size_offset) : size_offset_(size_offset) {}

    template <std::ranges::contiguous_range Range>
    auto operator()(Range&& range) const {
        return chunk_by_embedded_size_view<std::views::all_t<Range>>(std::views::all(std::forward<Range>(range)),
                                                                     size_offset_);
    }
};

// Factory function
inline auto chunk_by_embedded_size(std::size_t size_offset) {
    return chunk_by_embedded_size_adaptor{size_offset};
}

// Static assertions to verify concepts
static_assert(std::input_iterator<chunk_iterator<std::span<int>::iterator>>);
static_assert(std::forward_iterator<chunk_iterator<std::span<int>::iterator>>);
static_assert(std::ranges::forward_range<chunk_by_embedded_size_view<std::span<int>>>);

}  // namespace briefkasten
