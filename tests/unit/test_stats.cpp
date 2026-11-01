#include <gtest/gtest.h>

#include <sstream>

#include "config.hpp"
#include "interval_stats.hpp"
#include "server.hpp"
#include "stream_stats.hpp"
#include "token_bucket.hpp"

using namespace radar;

TEST(IntervalStats, EmptyIsAllZero) {
    IntervalStats s(35000);
    EXPECT_EQ(s.count(), 0u);
    EXPECT_EQ(s.mean_us(), 0.0);
    EXPECT_EQ(s.jitter_us(), 0.0);
}

TEST(IntervalStats, SummarizesAgainstTheTarget) {
    IntervalStats s(35000);
    s.add(34000);
    s.add(36000);
    s.add(35000);
    EXPECT_EQ(s.count(), 3u);
    EXPECT_EQ(s.min_us(), 34000);
    EXPECT_EQ(s.max_us(), 36000);
    EXPECT_DOUBLE_EQ(s.mean_us(), 35000.0);
    EXPECT_NEAR(s.jitter_us(), 2000.0 / 3.0, 0.001);  // the mean alone would hide this
}

TEST(IntervalStats, ResetKeepsTheTarget) {
    IntervalStats s(1000);
    s.add(5000);
    s.reset();
    EXPECT_EQ(s.count(), 0u);
    s.add(1000);
    EXPECT_EQ(s.min_us(), 1000);
    EXPECT_EQ(s.jitter_us(), 0.0);
}

TEST(StreamStats, ConsecutiveFramesAreNotLoss) {
    StreamStats s;
    for (std::uint32_t seq = 100; seq < 110; ++seq) s.on_frame(seq, 1000, 1500);
    EXPECT_EQ(s.received(), 10u);
    EXPECT_EQ(s.lost(), 0u);
    EXPECT_EQ(s.loss_percent(), 0.0);
}

TEST(StreamStats, GapsCountAsLoss) {
    StreamStats s;
    s.on_frame(10, 0, 0);
    s.on_frame(11, 0, 0);
    s.on_frame(15, 0, 0);  // 12, 13, 14 missing
    EXPECT_EQ(s.received(), 3u);
    EXPECT_EQ(s.lost(), 3u);
    EXPECT_DOUBLE_EQ(s.loss_percent(), 50.0);
}

TEST(StreamStats, LateFrameIsNotLoss) {
    StreamStats s;
    s.on_frame(1, 0, 0);
    s.on_frame(3, 0, 0);
    EXPECT_EQ(s.lost(), 1u);
    s.on_frame(2, 0, 0);
    EXPECT_EQ(s.lost(), 0u);
    EXPECT_EQ(s.reordered(), 1u);
}

TEST(StreamStats, NewSessionIsANewBaseline) {
    StreamStats s;
    s.on_frame(500, 0, 0);
    s.new_session();
    s.on_frame(900, 0, 0);  // the gap happened while disconnected
    EXPECT_EQ(s.lost(), 0u);
    s.new_session();
    s.on_frame(0, 0, 0);  // restarted server numbers from 0 again
    s.on_frame(1, 0, 0);
    EXPECT_EQ(s.lost(), 0u);
    EXPECT_EQ(s.reordered(), 0u);
}

TEST(StreamStats, Latency) {
    StreamStats s;
    s.on_frame(0, 1000, 1200);
    s.on_frame(1, 2000, 2600);
    EXPECT_EQ(s.latency_min_us(), 200);
    EXPECT_EQ(s.latency_max_us(), 600);
    EXPECT_DOUBLE_EQ(s.latency_mean_us(), 400.0);
}

TEST(StreamStats, ClockSkewGivesNegativeLatencyWithoutWrapping) {
    StreamStats s;
    s.on_frame(0, 5000, 1000);
    EXPECT_EQ(s.latency_min_us(), -4000);
}

TEST(StatsReport, ServerLineAndOneLinePerOccupiedSlot) {
    ServerStats st;
    st.frames_produced = 100;
    st.frames_dropped = 7;
    st.rejected_full = 2;

    std::vector<SlotSnapshot> slots(3);
    slots[0].index = 0;  // Free: no line
    slots[1].index = 1;
    slots[1].state = SlotState::Streaming;
    slots[1].peer = "127.0.0.1:5000";
    slots[1].udp_port = 6000;
    slots[1].frames_sent = 93;
    slots[1].frames_dropped = 7;
    slots[2].index = 2;

    IntervalStats window(35000);
    window.add(35000);

    const auto lines = format_stats_report(st, slots, window);
    ASSERT_EQ(lines.size(), 2u);
    EXPECT_NE(lines[0].find("clients=1/3"), std::string::npos) << lines[0];
    EXPECT_NE(lines[0].find("produced=100"), std::string::npos) << lines[0];
    EXPECT_NE(lines[0].find("dropped=7"), std::string::npos) << lines[0];
    EXPECT_NE(lines[0].find("avg=35.00"), std::string::npos) << lines[0];
    EXPECT_NE(lines[1].find("slot 1 STREAMING"), std::string::npos) << lines[1];
    EXPECT_NE(lines[1].find("frames_sent=93"), std::string::npos) << lines[1];
}

TEST(TokenBucket, AllowsABurstThenTheRate) {
    TokenBucket b(100);
    const Clock::time_point t0{};
    b.reset(t0);
    for (int i = 0; i < 100; ++i) EXPECT_TRUE(b.try_take(t0)) << i;
    EXPECT_FALSE(b.try_take(t0));

    EXPECT_FALSE(b.try_take(t0 + std::chrono::milliseconds(5)));
    EXPECT_TRUE(b.try_take(t0 + std::chrono::milliseconds(10)));  // 100/s = one per 10 ms
    EXPECT_FALSE(b.try_take(t0 + std::chrono::milliseconds(10)));
}

TEST(TokenBucket, IdleTimeDoesNotBuildUpMoreThanOneBurst) {
    TokenBucket b(10);
    const Clock::time_point t0{};
    b.reset(t0);
    const auto later = t0 + std::chrono::hours(1);
    for (int i = 0; i < 10; ++i) EXPECT_TRUE(b.try_take(later)) << i;
    EXPECT_FALSE(b.try_take(later));
}

TEST(TokenBucket, NextAvailableIsWhenATokenExists) {
    TokenBucket b(100);
    const Clock::time_point t0{};
    b.reset(t0);
    EXPECT_EQ(b.next_available(t0), t0);
    for (int i = 0; i < 100; ++i) b.try_take(t0);
    const auto at = b.next_available(t0);
    EXPECT_GT(at, t0);
    EXPECT_TRUE(b.try_take(at));
}

TEST(TokenBucket, ZeroMeansUnlimited) {
    TokenBucket b(0);
    const Clock::time_point t0{};
    b.reset(t0);
    for (int i = 0; i < 100000; ++i) ASSERT_TRUE(b.try_take(t0));
}

TEST(Config, CommandLimitMustLeaveRoomForHeartbeats) {
    std::string err;
    std::istringstream too_low("max_commands_per_sec = 1\nheartbeat_interval_ms = 1000\n");
    EXPECT_FALSE(parse_config(too_low, &err));
    EXPECT_NE(err.find("max_commands_per_sec"), std::string::npos) << err;

    std::istringstream enough("max_commands_per_sec = 2\nheartbeat_interval_ms = 1000\n");
    EXPECT_TRUE(parse_config(enough, &err)) << err;

    std::istringstream off("max_commands_per_sec = 0\n");
    EXPECT_TRUE(parse_config(off, &err)) << err;
}

TEST(Config, StatsInterval) {
    std::string err;
    std::istringstream on("stats_interval_ms = 250\n");
    auto cfg = parse_config(on, &err);
    ASSERT_TRUE(cfg) << err;
    EXPECT_EQ(cfg->stats_interval.count(), 250);

    std::istringstream off("stats_interval_ms = 0\n");
    cfg = parse_config(off, &err);
    ASSERT_TRUE(cfg) << err;
    EXPECT_EQ(cfg->stats_interval.count(), 0);
}
