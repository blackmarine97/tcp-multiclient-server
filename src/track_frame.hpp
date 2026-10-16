#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <random>
#include <vector>

namespace radar {

// ---- Track data -------------------------------------------------------------

struct Track {
    std::uint32_t id = 0;
    float range_m = 0;
    float azimuth_deg = 0;
    float elevation_deg = 0;
    float velocity_mps = 0;
    float rcs_dbsm = 0;
};
inline constexpr std::size_t kTrackWireSize = 24;

struct TrackFrame {
    std::uint32_t seq = 0;
    std::uint64_t timestamp_us = 0;
    std::vector<Track> tracks;
};
inline constexpr std::size_t kFrameHeaderWireSize = 16;  // seq u32, timestamp u64, count u32

// Deterministic fake radar: N targets moving in range and rotating in azimuth.
class FakeTrackGenerator {
public:
    explicit FakeTrackGenerator(std::size_t n_tracks, std::uint32_t seed = 42);
    TrackFrame next(std::uint64_t timestamp_us, double dt_seconds);

private:
    struct Target {
        float range, azimuth, elevation, velocity, omega, rcs;
    };
    std::vector<Target> targets_;
    std::uint32_t seq_ = 0;
};

// Little-endian wire format.
std::vector<std::uint8_t> serialize_frame(const TrackFrame& frame);
std::optional<TrackFrame> deserialize_frame(const std::uint8_t* data, std::size_t len);

// ---- UDP fragmentation ------------------------------------------------------
//
// Each datagram = 20-byte header + a slice of the serialized frame.
//   magic u32 "RDR1" | frame_seq u32 | frag_index u16 | frag_count u16 |
//   total_size u32   | offset u32
inline constexpr std::uint32_t kFragmentMagic = 0x31524452;  // bytes 'R','D','R','1'
inline constexpr std::size_t kFragmentHeaderSize = 20;

struct FragmentHeader {
    std::uint32_t frame_seq = 0;
    std::uint16_t index = 0;
    std::uint16_t count = 0;
    std::uint32_t total_size = 0;
    std::uint32_t offset = 0;
    std::size_t payload_len = 0;
};

std::size_t fragment_count_for(std::size_t payload_size, std::size_t max_datagram);
std::vector<std::vector<std::uint8_t>> fragment_payload(const std::vector<std::uint8_t>& payload,
                                                        std::uint32_t frame_seq, std::size_t max_datagram);
std::optional<FragmentHeader> parse_fragment(const std::uint8_t* data, std::size_t len);

// A frame serialized and cut into datagrams once, then shared (read-only) by every client queue.
struct PreparedFrame {
    std::uint32_t seq = 0;
    std::vector<std::vector<std::uint8_t>> fragments;
};
PreparedFrame prepare_frame(const TrackFrame& frame, std::size_t max_datagram);

// Receiver side: collects fragments until a frame is complete.
class FrameReassembler {
public:
    explicit FrameReassembler(std::size_t max_pending_frames = 16) : max_pending_(max_pending_frames) {}

    // Returns the complete serialized frame when `data` was its last missing fragment.
    std::optional<std::vector<std::uint8_t>> add(const std::uint8_t* data, std::size_t len);

    std::uint64_t fragments() const { return fragments_; }
    std::uint64_t frames_completed() const { return completed_; }
    std::uint64_t frames_abandoned() const { return abandoned_; }
    std::uint64_t invalid() const { return invalid_; }
    std::uint64_t duplicates() const { return duplicates_; }

private:
    struct Partial {
        std::vector<std::uint8_t> data;
        std::vector<bool> have;
        std::uint16_t received = 0;
    };
    std::size_t max_pending_;
    std::map<std::uint32_t, Partial> pending_;
    std::uint64_t fragments_ = 0, completed_ = 0, abandoned_ = 0, invalid_ = 0, duplicates_ = 0;
};

}  // namespace radar
