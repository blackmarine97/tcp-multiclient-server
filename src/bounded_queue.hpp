#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>

namespace radar {

// What to do when a producer pushes into a full queue.
//  - DropOldest: evict the head and accept the new item (latest data wins).
//  - DropNewest: reject the new item and keep what is queued.
// Either way the producer never blocks, and the discarded item is counted.
enum class OverflowPolicy { DropOldest, DropNewest };

enum class PushResult { Ok, DroppedOldest, DroppedNewest, Closed };

// Fixed-capacity multi-producer / multi-consumer queue.
// push() never blocks; pop() blocks until an item arrives or the queue is closed.
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity, OverflowPolicy policy = OverflowPolicy::DropOldest)
        : capacity_(capacity == 0 ? 1 : capacity), policy_(policy) {}

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    PushResult push(T item) {
        PushResult result = PushResult::Ok;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (closed_) return PushResult::Closed;
            if (items_.size() >= capacity_) {
                ++dropped_;
                if (policy_ == OverflowPolicy::DropNewest) return PushResult::DroppedNewest;
                items_.pop_front();
                result = PushResult::DroppedOldest;
            }
            items_.push_back(std::move(item));
        }
        cv_.notify_one();
        return result;
    }

    // Blocks until an item is available. Returns nullopt once closed and drained.
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [&] { return closed_ || !items_.empty(); });
        return take_locked();
    }

    template <typename Rep, typename Period>
    std::optional<T> pop_for(std::chrono::duration<Rep, Period> timeout) {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait_for(lk, timeout, [&] { return closed_ || !items_.empty(); });
        return take_locked();
    }

    // Discards queued items (not counted as drops). Returns how many were discarded.
    std::size_t clear() {
        std::lock_guard<std::mutex> lk(mtx_);
        std::size_t n = items_.size();
        items_.clear();
        return n;
    }

    // Wakes every blocked consumer; later pushes are refused.
    void close() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            closed_ = true;
        }
        cv_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return items_.size();
    }

    std::size_t capacity() const { return capacity_; }

    std::uint64_t dropped() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return dropped_;
    }

private:
    std::optional<T> take_locked() {
        if (items_.empty()) return std::nullopt;
        T item = std::move(items_.front());
        items_.pop_front();
        return item;
    }

    const std::size_t capacity_;
    const OverflowPolicy policy_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<T> items_;
    std::uint64_t dropped_ = 0;
    bool closed_ = false;
};

}  // namespace radar
