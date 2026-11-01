#include <gtest/gtest.h>

#include "backoff.hpp"
#include "track_frame.hpp"

using namespace radar;
using namespace std::chrono_literals;

TEST(ReconnectBackoff, DoublesUpToTheCap) {
    ReconnectBackoff b(1000ms, 8000ms);
    EXPECT_EQ(b.next_delay(), 1000ms);
    EXPECT_EQ(b.next_delay(), 2000ms);
    EXPECT_EQ(b.next_delay(), 4000ms);
    EXPECT_EQ(b.next_delay(), 8000ms);
    EXPECT_EQ(b.next_delay(), 8000ms);
    EXPECT_EQ(b.failures(), 5);
}

TEST(ReconnectBackoff, ResetStartsOver) {
    ReconnectBackoff b(100ms, 1000ms);
    b.next_delay();
    b.next_delay();
    b.reset();
    EXPECT_EQ(b.failures(), 0);
    EXPECT_EQ(b.next_delay(), 100ms);
}

TEST(ReconnectBackoff, CapBelowMinimumUsesMinimum) {
    ReconnectBackoff b(500ms, 100ms);
    EXPECT_EQ(b.next_delay(), 500ms);
    EXPECT_EQ(b.next_delay(), 500ms);
}

TEST(FrameReassembler, DiscardPendingKeepsOldPiecesOutOfNewFrames) {
    FakeTrackGenerator gen(64);
    const auto frame = prepare_frame(gen.next(0, 0.035), 512);
    ASSERT_EQ(frame.fragments.size(), 4u);

    FrameReassembler r;
    r.add(frame.fragments[0].data(), frame.fragments[0].size());
    r.discard_pending();
    EXPECT_EQ(r.frames_abandoned(), 1u);

    // After the discard the same frame number needs all of its fragments again.
    for (std::size_t i = 1; i < 4; ++i) r.add(frame.fragments[i].data(), frame.fragments[i].size());
    EXPECT_EQ(r.frames_completed(), 0u);
    EXPECT_TRUE(r.add(frame.fragments[0].data(), frame.fragments[0].size()).has_value());
    EXPECT_EQ(r.frames_completed(), 1u);
}
