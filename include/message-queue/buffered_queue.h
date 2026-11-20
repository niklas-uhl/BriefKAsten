#pragma once

#include <cstddef>
#include <functional>
#include <type_traits>
#include <unordered_map>
#include "message-queue/debug_print.h"
#include "message-queue/queue.h"

namespace message_queue {

template <class T, typename Merger, typename Splitter>
class BufferedMessageQueue {
    // static_assert(std::is_invocable_v<
    //               Splitter,
    //               std::vector<T>&,
    //               std::function<void(typename std::vector<T>::const_iterator, typename
    //               std::vector<T>::const_iterator, PEID)>, PEID>);
    // static_assert(std::is_invocable_r_v<size_t, Merger, std::vector<T>&, std::vector<T>, int>);

public:
    using value_type = T;
    BufferedMessageQueue(Merger&& merge, Splitter&& split)
        : queue_(),
          buffers_(),
          buffer_ocupacy_(0),
          threshold_(std::numeric_limits<size_t>::max()),
          overflows_(0),
          merge(merge),
          split(split) {}

    /// `message` is handed to the merger as is: any type the merger accepts (e.g. a std::vector<T>, or a view of a
    /// message that is sent to several receivers, which the merger appends without an intermediate copy).
    template <typename Message>
    void post_message(Message&& message, PEID receiver, int tag = 0) {
        auto& buffer = buffers_[receiver];
        size_t added_elements = merge(buffer, std::forward<Message>(message), tag);
        buffer_ocupacy_ += added_elements;
        if (buffer_ocupacy_ > threshold_) {
            overflows_++;
            flush_all();
        }
        // atomic_debug(buffer);
    }

    void set_threshold(size_t threshold) {
        threshold_ = threshold;
        if (buffer_ocupacy_ > threshold_) {
            overflows_++;
            flush_all();
        }
    }
    size_t threshold() const {
        return threshold_;
    }

    void flush(PEID receiver) {
        auto& buffer = buffers_[receiver];
        if (!buffer.empty()) {
            size_t buffer_size = buffer.size();
            queue_.post_message(std::move(buffer), receiver);
            buffer_ocupacy_ -= buffer_size;
        }
    }

    void flush_all() {
        for (auto& kv : buffers_) {
            if (!kv.second.empty()) {
                size_t buffer_size = kv.second.size();
                queue_.post_message(std::move(kv.second), kv.first);
                buffer_ocupacy_ -= buffer_size;
            }
        }
    }

    template <typename MessageHandler>
    bool poll(MessageHandler&& on_message) {
        // static_assert(std::is_invocable_v<MessageHandler, typename std::vector<T>::const_iterator,
        //                                   typename std::vector<T>::const_iterator, PEID>);
        return queue_.poll([&](std::vector<T> message, PEID sender) { split(message, on_message, sender); });
    }

    template <typename MessageHandler>
    void terminate(MessageHandler&& on_message) {
        // static_assert(std::is_invocable_v<MessageHandler, typename std::vector<T>::iterator,
        //                                   typename std::vector<T>::iterator, PEID>);
        queue_.terminate_impl([&](std::vector<T> message, PEID sender) { split(message, on_message, sender); },
                              [&]() { flush_all(); });
        /* for (auto buffer : buffers_) { */
        /*     atomic_debug(buffer); */
        /* } */
    }

    template <typename MessageHandler>
    bool try_terminate(MessageHandler&& on_message) {
        // static_assert(std::is_invocable_v<MessageHandler, typename std::vector<T>::iterator,
        //  typename std::vector<T>::iterator, PEID>);
        // atomic_debug("Try terminate");
        return queue_.try_terminate_impl(
            [&](std::vector<T> message, PEID sender) { split(message, on_message, sender); }, [&]() { flush_all(); });
        /* for (auto buffer : buffers_) { */
        /*     atomic_debug(buffer); */
        /* } */
    }

    void reactivate() {
        queue_.reactivate();
    }

    void check_for_overflow_and_flush() {
        if (buffer_ocupacy() > threshold_) {
            overflows_++;
            flush_all();
        }
    }

    size_t overflows() const {
        return overflows_;
    }

    const MessageStatistics& stats() {
        return queue_.stats();
    }

    void reset() {
        queue_.reset();
        buffers_.clear();
        buffer_ocupacy_ = 0;
        overflows_ = 0;
    }

    size_t buffer_ocupacy() const {
        return buffer_ocupacy_;
    }

private:
    MessageQueue<T> queue_;
    std::unordered_map<PEID, std::vector<T>> buffers_;
    size_t buffer_ocupacy_;
    size_t threshold_;
    size_t overflows_;
    Merger merge;
    Splitter split;
};

template <class T, typename Merger, typename Splitter>
auto make_buffered_queue(Merger&& merger, Splitter&& splitter) {
    return BufferedMessageQueue<T, Merger, Splitter>(std::forward<Merger>(merger), std::forward<Splitter>(splitter));
}

}  // namespace message_queue
