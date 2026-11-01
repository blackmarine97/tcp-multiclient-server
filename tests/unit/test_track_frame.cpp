#include <gtest/gtest.h>

#include <algorithm>

#include "track_frame.hpp"

using namespace radar;

TEST(TrackFrame, SerializeRoundTrip) {
    FakeTrackGenerator gen(10);
    const TrackFrame f = gen.next(123456789, 0.035);
    const auto bytes = serialize_frame(f);
    EXPECT_EQ(bytes.size(), kFrameHeaderWireSize + 10 * kTrackWireSize);

    const auto back = deserialize_frame(bytes.data(), bytes.size());
    ASSERT_TRUE(back);
    EXPECT_EQ(back->seq, f.seq);
    EXPECT_EQ(back->timestamp_us, 123456789u);
    ASSERT_EQ(back->tracks.size(), 10u);
    EXPECT_EQ(back->tracks[3].id, f.tracks[3].id);
    EXPECT_FLOAT_EQ(back->tracks[3].range_m, f.tracks[3].range_m);
    EXPECT_FLOAT_EQ(back->tracks[9].rcs_dbsm, f.tracks[9].rcs_dbsm);
}

TEST(TrackFrame, DeserializeRejectsTruncated) {
    FakeTrackGenerator gen(2);
    auto bytes = serialize_frame(gen.next(0, 0.035));
    bytes.pop_back();
    EXPECT_FALSE(deserialize_frame(bytes.data(), bytes.size()));
}

TEST(TrackFrame, GeneratorIsDeterministicAndSequenced) {
    FakeTrackGenerator a(5, 7), b(5, 7);
    for (int i = 0; i < 3; ++i) {
        auto fa = a.next(0, 0.035), fb = b.next(0, 0.035);
        EXPECT_EQ(fa.seq, static_cast<std::uint32_t>(i));
        EXPECT_EQ(serialize_frame(fa), serialize_frame(fb));
    }
}

TEST(Fragmentation, CountMatchesDefaultConfig) {
    // 64 tracks -> 1552 bytes; 512-byte datagrams carry 492 bytes each -> 4 fragments.
    EXPECT_EQ(fragment_count_for(kFrameHeaderWireSize + 64 * kTrackWireSize, 512), 4u);
    EXPECT_EQ(fragment_count_for(0, 512), 1u);
    EXPECT_EQ(fragment_count_for(492, 512), 1u);
    EXPECT_EQ(fragment_count_for(493, 512), 2u);
}

TEST(Fragmentation, EveryDatagramFitsAndHeaderParses) {
    FakeTrackGenerator gen(64);
    const auto p = prepare_frame(gen.next(0, 0.035), 512);
    ASSERT_EQ(p.fragments.size(), 4u);
    for (std::size_t i = 0; i < p.fragments.size(); ++i) {
        EXPECT_LE(p.fragments[i].size(), 512u);
        auto h = parse_fragment(p.fragments[i].data(), p.fragments[i].size());
        ASSERT_TRUE(h);
        EXPECT_EQ(h->index, i);
        EXPECT_EQ(h->count, 4);
        EXPECT_EQ(h->total_size, 1552u);
    }
}

TEST(Fragmentation, ReassemblesOutOfOrderAndIgnoresDuplicates) {
    FakeTrackGenerator gen(64);
    const TrackFrame f = gen.next(42, 0.035);
    auto frags = fragment_payload(serialize_frame(f), f.seq, 512);
    std::reverse(frags.begin(), frags.end());

    FrameReassembler r;
    std::optional<std::vector<std::uint8_t>> done;
    for (std::size_t i = 0; i < frags.size(); ++i) {
        done = r.add(frags[i].data(), frags[i].size());
        if (i == 0) EXPECT_FALSE(r.add(frags[0].data(), frags[0].size()));  // duplicate
        if (i + 1 < frags.size()) EXPECT_FALSE(done);
    }
    ASSERT_TRUE(done);
    EXPECT_EQ(*done, serialize_frame(f));
    EXPECT_EQ(r.frames_completed(), 1u);
    EXPECT_EQ(r.duplicates(), 1u);
    EXPECT_EQ(r.fragments(), frags.size() + 1);
}

TEST(Fragmentation, DuplicateAfterCompletionDoesNotStartAPhantomFrame) {
    FakeTrackGenerator gen(64);
    const auto p = prepare_frame(gen.next(0, 0.035), 512);
    FrameReassembler r;
    for (const auto& f : p.fragments) r.add(f.data(), f.size());
    ASSERT_EQ(r.frames_completed(), 1u);

    for (const auto& f : p.fragments) EXPECT_FALSE(r.add(f.data(), f.size()));
    EXPECT_EQ(r.frames_completed(), 1u);  // not counted twice
    EXPECT_EQ(r.duplicates(), p.fragments.size());
    r.discard_pending();
    EXPECT_EQ(r.frames_abandoned(), 0u);  // nothing was left half-built
}

TEST(Fragmentation, DiscardPendingForgetsCompletedFramesForARestartedServer) {
    FakeTrackGenerator first(64), restarted(64);
    const auto a = prepare_frame(first.next(0, 0.035), 512);
    const auto b = prepare_frame(restarted.next(0, 0.035), 512);
    ASSERT_EQ(a.seq, b.seq);

    FrameReassembler r;
    for (const auto& f : a.fragments) r.add(f.data(), f.size());
    r.discard_pending();
    for (const auto& f : b.fragments) r.add(f.data(), f.size());
    EXPECT_EQ(r.frames_completed(), 2u);
}

TEST(Fragmentation, RejectsGarbage) {
    FrameReassembler r;
    std::vector<std::uint8_t> junk(40, 0xAB);
    EXPECT_FALSE(r.add(junk.data(), junk.size()));
    EXPECT_EQ(r.invalid(), 1u);
    EXPECT_EQ(r.fragments(), 0u);
}

TEST(Fragmentation, AbandonsOldIncompleteFrames) {
    FakeTrackGenerator gen(64);
    FrameReassembler r(2);
    for (int i = 0; i < 3; ++i) {  // only the first fragment of 3 frames
        auto p = prepare_frame(gen.next(0, 0.035), 512);
        r.add(p.fragments[0].data(), p.fragments[0].size());
    }
    EXPECT_EQ(r.frames_abandoned(), 1u);
    EXPECT_EQ(r.frames_completed(), 0u);
}
