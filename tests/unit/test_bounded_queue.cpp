#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "bounded_queue.hpp"

using namespace radar;
using namespace std::chrono_literals;

TEST(BoundedQueue, FifoOrder) {
    BoundedQueue<int> q(4);
    for (int i = 1; i <= 3; ++i) EXPECT_EQ(q.push(i), PushResult::Ok);
    EXPECT_EQ(q.size(), 3u);
    EXPECT_EQ(*q.pop(), 1);
    EXPECT_EQ(*q.pop(), 2);
    EXPECT_EQ(*q.pop(), 3);
    EXPECT_EQ(q.dropped(), 0u);
}

TEST(BoundedQueue, DropOldestKeepsNewestAndCounts) {
    BoundedQueue<int> q(3, OverflowPolicy::DropOldest);
    for (int i = 1; i <= 3; ++i) EXPECT_EQ(q.push(i), PushResult::Ok);
    EXPECT_EQ(q.push(4), PushResult::DroppedOldest);
    EXPECT_EQ(q.push(5), PushResult::DroppedOldest);
    EXPECT_EQ(q.size(), 3u);
    EXPECT_EQ(q.dropped(), 2u);
    EXPECT_EQ(*q.pop(), 3);
    EXPECT_EQ(*q.pop(), 4);
    EXPECT_EQ(*q.pop(), 5);
}

TEST(BoundedQueue, DropNewestKeepsQueuedAndCounts) {
    BoundedQueue<int> q(3, OverflowPolicy::DropNewest);
    for (int i = 1; i <= 3; ++i) q.push(i);
    EXPECT_EQ(q.push(4), PushResult::DroppedNewest);
    EXPECT_EQ(q.dropped(), 1u);
    EXPECT_EQ(*q.pop(), 1);
    EXPECT_EQ(*q.pop(), 2);
    EXPECT_EQ(*q.pop(), 3);
}

TEST(BoundedQueue, ZeroCapacityBecomesOne) {
    BoundedQueue<int> q(0);
    EXPECT_EQ(q.capacity(), 1u);
}

TEST(BoundedQueue, CloseWakesBlockedConsumer) {
    BoundedQueue<int> q(2);
    std::atomic<bool> returned{false};
    std::thread consumer([&] {
        EXPECT_FALSE(q.pop().has_value());
        returned = true;
    });
    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(returned.load());
    q.close();
    consumer.join();
    EXPECT_TRUE(returned.load());
}

TEST(BoundedQueue, ClosedQueueDrainsThenRefuses) {
    BoundedQueue<int> q(4);
    q.push(1);
    q.push(2);
    q.close();
    EXPECT_EQ(q.push(3), PushResult::Closed);
    EXPECT_EQ(*q.pop(), 1);
    EXPECT_EQ(*q.pop(), 2);
    EXPECT_FALSE(q.pop().has_value());
}

TEST(BoundedQueue, ClearIsNotCountedAsDrop) {
    BoundedQueue<int> q(4);
    q.push(1);
    q.push(2);
    EXPECT_EQ(q.clear(), 2u);
    EXPECT_EQ(q.size(), 0u);
    EXPECT_EQ(q.dropped(), 0u);
}

TEST(BoundedQueue, PopForTimesOut) {
    BoundedQueue<int> q(1);
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(q.pop_for(30ms).has_value());
    EXPECT_GE(std::chrono::steady_clock::now() - t0, 25ms);
}

// Many producers, one slow consumer, tiny queue: nothing is lost without being counted.
TEST(BoundedQueue, ConcurrentProducersAccountForEveryItem) {
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 20000;
    BoundedQueue<int> q(16, OverflowPolicy::DropOldest);

    std::atomic<long> consumed{0};
    std::thread consumer([&] {
        while (q.pop()) ++consumed;
    });
    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p)
        producers.emplace_back([&] {
            for (int i = 0; i < kPerProducer; ++i) q.push(i);
        });
    for (auto& t : producers) t.join();
    q.close();
    consumer.join();

    EXPECT_EQ(consumed.load() + static_cast<long>(q.dropped()), kProducers * kPerProducer);
}
