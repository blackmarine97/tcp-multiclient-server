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

TEST(TrackFrame, EmptyFrameRoundTrip) {
    FakeTrackGenerator gen(0);
    const auto bytes = serialize_frame(gen.next(5, 0.035));
    ASSERT_EQ(bytes.size(), kFrameHeaderWireSize);
    const auto back = deserialize_frame(bytes.data(), bytes.size());
    ASSERT_TRUE(back);
    EXPECT_TRUE(back->tracks.empty());
}

TEST(TrackFrame, DeserializeRejectsTrailingBytesAndImpossibleCounts) {
    FakeTrackGenerator gen(2);
    auto bytes = serialize_frame(gen.next(0, 0.035));
    bytes.push_back(0);
    EXPECT_FALSE(deserialize_frame(bytes.data(), bytes.size()));
    bytes.pop_back();

    for (int i = 12; i < 16; ++i) bytes[static_cast<std::size_t>(i)] = 0xFF;  // count = 2^32 - 1
    EXPECT_FALSE(deserialize_frame(bytes.data(), bytes.size()));
    EXPECT_FALSE(deserialize_frame(bytes.data(), 0));
}

TEST(TrackFrame, GeneratorStaysInItsPhysicalRange) {
    FakeTrackGenerator gen(64);
    for (int i = 0; i < 20000; ++i) {  // 700 s of motion: every target bounces at least once
        for (const Track& t : gen.next(0, 0.035).tracks) {
            ASSERT_GE(t.range_m, 480.f);
            ASSERT_LE(t.range_m, 30020.f);
            ASSERT_GE(t.azimuth_deg, 0.f);
            ASSERT_LT(t.azimuth_deg, 360.f);
        }
    }
}

// ---- Datagrams nobody should have sent: UDP has no sender check, so any host can send these. ----

namespace {

std::vector<std::uint8_t> forged_fragment(std::uint32_t seq, std::uint16_t index, std::uint16_t count,
                                          std::uint32_t total_size, std::uint32_t offset, std::size_t payload_len) {
    std::vector<std::uint8_t> d;
    const auto put = [&d](std::uint32_t v, int bytes) {
        for (int i = 0; i < bytes; ++i) d.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
    };
    put(kFragmentMagic, 4);
    put(seq, 4);
    put(index, 2);
    put(count, 2);
    put(total_size, 4);
    put(offset, 4);
    d.resize(d.size() + payload_len, 0x5A);
    return d;
}

std::optional<std::vector<std::uint8_t>> add(FrameReassembler& r, const std::vector<std::uint8_t>& d) {
    return r.add(d.data(), d.size());
}

}  // namespace

TEST(ForgedFragment, HugeTotalSizeIsRejectedWithoutAllocating) {
    FrameReassembler r;
    EXPECT_FALSE(add(r, forged_fragment(1, 0, 1, 0xFFFFFFFFu, 0, 0)));
    EXPECT_FALSE(add(r, forged_fragment(2, 0, 2, 0xFFFFFFFFu, 0, 100)));
    EXPECT_EQ(r.invalid(), 2u);
    EXPECT_EQ(r.fragments(), 0u);
    r.discard_pending();
    EXPECT_EQ(r.frames_abandoned(), 0u);  // nothing was kept
}

TEST(ForgedFragment, TheLargestFrameTheServerCanSendStillGetsThrough) {
    FakeTrackGenerator gen(10000);
    const auto frame = gen.next(0, 0.035);
    const auto p = prepare_frame(frame, 512);
    FrameReassembler r;
    std::optional<std::vector<std::uint8_t>> done;
    for (const auto& f : p.fragments) done = r.add(f.data(), f.size());
    ASSERT_TRUE(done);
    EXPECT_EQ(*done, serialize_frame(frame));
    EXPECT_EQ(r.invalid(), 0u);
}

TEST(ForgedFragment, OffsetsThatLeaveHolesNeverCompleteAFrame) {
    FrameReassembler r;
    for (std::uint16_t i = 0; i < 4; ++i)  // every fragment claims offset 0
        EXPECT_FALSE(add(r, forged_fragment(1, i, 4, 400, 0, 100)));
    EXPECT_EQ(r.frames_completed(), 0u);
}

TEST(ForgedFragment, FragmentsOfDifferentSizesNeverCompleteAFrame) {
    FrameReassembler r;
    EXPECT_FALSE(add(r, forged_fragment(1, 0, 2, 100, 0, 60)));   // covers 0..60
    EXPECT_FALSE(add(r, forged_fragment(1, 1, 2, 100, 90, 10)));  // covers 90..100, 60..90 is a hole
    EXPECT_EQ(r.frames_completed(), 0u);
    EXPECT_EQ(r.invalid(), 1u);
}

TEST(ForgedFragment, HeaderThatContradictsItselfIsRejected) {
    const auto rejected = [](const std::vector<std::uint8_t>& d) {
        return !parse_fragment(d.data(), d.size());
    };
    EXPECT_TRUE(rejected(forged_fragment(1, 0, 1, 100, 0, 99)));    // single fragment, too short
    EXPECT_TRUE(rejected(forged_fragment(1, 0, 1, 100, 1, 99)));    // single fragment, not at 0
    EXPECT_TRUE(rejected(forged_fragment(1, 0, 2, 100, 0, 0)));     // no progress
    EXPECT_TRUE(rejected(forged_fragment(1, 0, 2, 100, 0, 200)));   // past the end
    EXPECT_TRUE(rejected(forged_fragment(1, 0, 2, 100, 0, 40)));    // two of these cannot reach 100
    EXPECT_TRUE(rejected(forged_fragment(1, 0, 3, 100, 10, 40)));   // first fragment not at 0
    EXPECT_TRUE(rejected(forged_fragment(1, 1, 2, 100, 90, 20)));   // last one runs past the end
    EXPECT_TRUE(rejected(forged_fragment(1, 1, 2, 100, 90, 5)));    // last one stops short
    EXPECT_TRUE(rejected(forged_fragment(1, 2, 3, 100, 91, 9)));    // offset not a multiple of the index
    EXPECT_TRUE(rejected(forged_fragment(1, 2, 2, 100, 0, 50)));    // index out of range
    EXPECT_TRUE(rejected(forged_fragment(1, 0, 0, 100, 0, 50)));    // no fragments at all
}

TEST(ForgedFragment, FragmentThatDisagreesWithItsFrameDoesNotDisturbIt) {
    FakeTrackGenerator gen(64);
    const TrackFrame f = gen.next(0, 0.035);
    const auto p = prepare_frame(f, 512);
    FrameReassembler r;
    add(r, p.fragments[0]);

    EXPECT_FALSE(add(r, forged_fragment(f.seq, 1, 8, 1552, 196, 196)));  // same seq, other count
    EXPECT_FALSE(add(r, forged_fragment(f.seq, 1, 2, 300, 150, 150)));   // same seq, other size
    EXPECT_EQ(r.invalid(), 2u);

    std::optional<std::vector<std::uint8_t>> done;
    for (std::size_t i = 1; i < p.fragments.size(); ++i) done = add(r, p.fragments[i]);
    ASSERT_TRUE(done);
    EXPECT_EQ(*done, serialize_frame(f));
}

TEST(Fragmentation, EveryPayloadAndDatagramSizeRoundTrips) {
    for (std::size_t size = 0; size <= 60; ++size) {
        for (std::size_t datagram = kFragmentHeaderSize + 1; datagram <= kFragmentHeaderSize + 12; ++datagram) {
            std::vector<std::uint8_t> payload(size);
            for (std::size_t i = 0; i < size; ++i) payload[i] = static_cast<std::uint8_t>(i + 1);
            auto frags = fragment_payload(payload, 7, datagram);
            std::reverse(frags.begin(), frags.end());

            FrameReassembler r;
            std::optional<std::vector<std::uint8_t>> done;
            for (const auto& f : frags) done = add(r, f);
            ASSERT_TRUE(done) << "size=" << size << " datagram=" << datagram;
            EXPECT_EQ(*done, payload);
            EXPECT_EQ(r.invalid(), 0u);
        }
    }
}

TEST(Fragmentation, OldestFrameIsAbandonedAcrossSequenceWraparound) {
    FrameReassembler r(2);
    const std::vector<std::uint8_t> payload(100, 1);
    std::vector<std::vector<std::uint8_t>> newest;
    for (std::uint32_t seq : {0xFFFFFFFEu, 0xFFFFFFFFu, 0u, 1u}) {
        newest = fragment_payload(payload, seq, kFragmentHeaderSize + 50);
        add(r, newest[0]);
    }
    EXPECT_EQ(r.frames_abandoned(), 2u);

    // 0 and 1 are the newest two, so both must still be waiting for their second half.
    EXPECT_TRUE(add(r, newest[1]));
    EXPECT_TRUE(add(r, fragment_payload(payload, 0u, kFragmentHeaderSize + 50)[1]));
}
