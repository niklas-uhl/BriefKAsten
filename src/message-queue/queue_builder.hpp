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
#include "./aggregators.hpp"
#include "./buffered_queue.hpp"
#include "./detail/concepts.hpp"

namespace message_queue {

template <typename MessageType,
          typename BufferType = MessageType,
          typename BufferContainer = std::vector<BufferType>,
          typename ReceiveBufferContainer = std::vector<BufferType>,
          typename Merger = aggregation::AppendMerger,
          typename Splitter = aggregation::NoSplitter,
          typename BufferCleaner = aggregation::NoOpCleaner>
class BufferedMessageQueueBuilder {
private:
    BufferedMessageQueueBuilder(MPI_Comm comm, Config config, Merger merger, Splitter splitter, BufferCleaner cleaner)
        : config_(config),
          comm_(comm),

          merger_(std::move(merger)),
          splitter_(std::move(splitter)),
          cleaner_(std::move(cleaner)) {}

    template <typename MessageType_,
              typename BufferType_,
              typename BufferContainer_,
              typename ReceiveBufferContainer_,
              typename Merger_,
              typename Splitter_,
              typename BufferCleaner_>
    friend class BufferedMessageQueueBuilder;  // Allow chaining of builder methods

public:
    explicit BufferedMessageQueueBuilder(MPI_Comm comm, Config config = {}) : config_(config), comm_(comm) {}
    explicit BufferedMessageQueueBuilder(Config config = {}, MPI_Comm comm = MPI_COMM_WORLD)
        : config_(config), comm_(comm) {}

    template <typename Merger_>
        requires aggregation::Merger<Merger_, MessageType, BufferContainer>
    [[nodiscard]] auto with_merger(Merger_ merger) {
        return BufferedMessageQueueBuilder<MessageType, BufferType, BufferContainer, ReceiveBufferContainer, Merger_,
                                           Splitter, BufferCleaner>{comm_, config_, std::move(merger),
                                                                    std::move(splitter_), std::move(cleaner_)};
    }
    template <typename Splitter_>
        requires aggregation::Splitter<Splitter_, MessageType, BufferContainer>
    [[nodiscard]] auto with_splitter(Splitter_ splitter) {
        return BufferedMessageQueueBuilder<MessageType, BufferType, BufferContainer, ReceiveBufferContainer, Merger,
                                           Splitter_, BufferCleaner>{comm_, config_, std::move(merger_),
                                                                     std::move(splitter), std::move(cleaner_)};
    }
    template <typename BufferCleaner_>
        requires aggregation::BufferCleaner<BufferCleaner_, BufferContainer>
    [[nodiscard]] auto with_buffer_cleaner(BufferCleaner_ cleaner) {
        return BufferedMessageQueueBuilder<MessageType, BufferType, BufferContainer, ReceiveBufferContainer, Merger,
                                           Splitter, BufferCleaner_>{comm_, config_, std::move(merger_),
                                                                     std::move(splitter_), std::move(cleaner)};
    }
    template <MPIType BufferType_,
              MPIBuffer<BufferType_> BufferContainer_ = std::vector<BufferType_>,
              MPIBuffer<BufferType_> ReceiveBufferContainer_ = std::vector<BufferType_>>
    [[nodiscard]] auto with_buffer_type() {
        return BufferedMessageQueueBuilder<MessageType, BufferType_, BufferContainer_, ReceiveBufferContainer_, Merger,
                                           Splitter, BufferCleaner>{comm_, config_, std::move(merger_),
                                                                    std::move(splitter_), std::move(cleaner_)};
    }

    [[nodiscard]] auto build() {
        return BufferedMessageQueue<MessageType, BufferType, BufferContainer, ReceiveBufferContainer, Merger, Splitter,
                                    BufferCleaner>(comm_, config_, std::move(merger_), std::move(splitter_),
                                                   std::move(cleaner_));
    }

private:
    Config config_;
    MPI_Comm comm_;
    Merger merger_{};
    Splitter splitter_{};
    BufferCleaner cleaner_{};
};
}  // namespace message_queue
