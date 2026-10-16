// Zombie judgement (좀비 판정) with a simulated clock.

#include <gtest/gtest.h>

#include <optional>

#include "liveness.hpp"

using namespace radar;
using namespace std::chrono_literals;

namespace {
const Clock::time_point T0{};  // arbitrary epoch for simulated time
constexpr Millis kHb = 1000ms;
constexpr Millis kTimeout = 3500ms;
}  // namespace

TEST(Liveness, AliveJustBeforeTimeout) {
    EXPECT_EQ(judge_liveness(T0, T0 + kTimeout - 1ms, kTimeout), Liveness::Alive);
}

TEST(Liveness, ZombieAtAndAfterTimeout) {
    EXPECT_EQ(judge_liveness(T0, T0 + kTimeout, kTimeout), Liveness::Zombie);
    EXPECT_EQ(judge_liveness(T0, T0 + 10s, kTimeout), Liveness::Zombie);
}

TEST(Liveness, AnyReceivedByteResetsTheClock) {
    const auto last_rx = T0 + 3000ms;  // e.g. a PONG arrived at t=3s
    EXPECT_EQ(judge_liveness(last_rx, T0 + 6000ms, kTimeout), Liveness::Alive);
    EXPECT_EQ(judge_liveness(last_rx, T0 + 6500ms, kTimeout), Liveness::Zombie);
}

TEST(Liveness, HeartbeatDue) {
    EXPECT_FALSE(heartbeat_due(T0, T0 + 999ms, kHb));
    EXPECT_TRUE(heartbeat_due(T0, T0 + 1000ms, kHb));
}

TEST(Liveness, NextDeadlineIsTheEarlierOfHeartbeatAndTimeout) {
    EXPECT_EQ(next_liveness_deadline(T0, T0, kHb, kTimeout), T0 + kHb);
    // Heartbeat just sent but client silent for a while: timeout comes first.
    EXPECT_EQ(next_liveness_deadline(T0, T0 + 3000ms, kHb, kTimeout), T0 + kTimeout);
}

// Simulates the server loop in 10 ms ticks: a client answering every heartbeat
// after a 200 ms delay is never declared a zombie.
TEST(Liveness, ResponsiveClientNeverBecomesZombie) {
    auto last_rx = T0, last_hb = T0;
    std::optional<Clock::time_point> pong_at;
    for (auto now = T0; now < T0 + 60s; now += 10ms) {
        if (pong_at && now >= *pong_at) {
            last_rx = now;
            pong_at.reset();
        }
        ASSERT_EQ(judge_liveness(last_rx, now, kTimeout), Liveness::Alive) << "at t=" << (now - T0).count();
        if (heartbeat_due(last_hb, now, kHb)) {
            last_hb = now;
            pong_at = now + 200ms;
        }
    }
}

// Same loop, client stops answering at t=5s: zombie exactly zombie_timeout after its last byte.
TEST(Liveness, SilentClientDetectedAfterTimeout) {
    auto last_rx = T0, last_hb = T0;
    const auto dies_at = T0 + 5s;
    std::optional<Clock::time_point> detected;
    for (auto now = T0; now < T0 + 20s && !detected; now += 10ms) {
        if (judge_liveness(last_rx, now, kTimeout) == Liveness::Zombie) {
            detected = now;
            break;
        }
        if (heartbeat_due(last_hb, now, kHb)) {
            last_hb = now;
            if (now < dies_at) last_rx = now;  // instant PONG while alive
        }
    }
    ASSERT_TRUE(detected);
    // Last answered heartbeat was at t=4s, so the zombie verdict lands at 4s + 3.5s.
    EXPECT_EQ(*detected, T0 + 4s + kTimeout);
}
