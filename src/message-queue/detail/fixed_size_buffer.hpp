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

#include <cstddef>
#include <memory>

namespace message_queue {

template <typename T>
class FixedSizeBuffer {
    std::unique_ptr<T[]> data_;
    std::size_t size_ = 0;

public:
    FixedSizeBuffer() : data_(nullptr) {}

    [[nodiscard]] auto size() const -> std::size_t {
        return size_;
    }

    void reserve(std::size_t size) {
        data_ = std::unique_ptr<T[]>{new T[size]};
    }

    void resize(std::size_t size) {
        size_ = size;
    }

    T* data() const {
        return data_.get();
    }

    [[nodiscard]] auto empty() const -> bool {
        return size() == 0;
    }

    void emplace_back(T&& val) {
        data_[size_++] = std::move(val);
    }

    auto begin() const {
        return data_.get();
    }

    auto end() const {
        return begin() + size();
    }
};

}  // namespace message_queue
