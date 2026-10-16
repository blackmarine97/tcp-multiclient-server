#include "track_frame.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>

namespace radar {

namespace {

void put_u16(std::vector<std::uint8_t>& out, std::uint16_t v) {
    out.push_back(static_cast<std::uint8_t>(v));
    out.push_back(static_cast<std::uint8_t>(v >> 8));
}
void put_u32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void put_u64(std::vector<std::uint8_t>& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}
void put_f32(std::vector<std::uint8_t>& out, float f) {
    std::uint32_t v;
    std::memcpy(&v, &f, sizeof v);
    put_u32(out, v);
}

std::uint16_t get_u16(const std::uint8_t* p) {
    return static_cast<std::uint16_t>(p[0] | (p[1] << 8));
}
std::uint32_t get_u32(const std::uint8_t* p) {
    std::uint32_t v = 0;
    for (int i = 3; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}
std::uint64_t get_u64(const std::uint8_t* p) {
    std::uint64_t v = 0;
    for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}
float get_f32(const std::uint8_t* p) {
    std::uint32_t v = get_u32(p);
    float f;
    std::memcpy(&f, &v, sizeof f);
    return f;
}

}  // namespace

// ---- FakeTrackGenerator -----------------------------------------------------

FakeTrackGenerator::FakeTrackGenerator(std::size_t n_tracks, std::uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> range(1000.f, 20000.f), az(0.f, 360.f), el(0.f, 10.f),
        vel(-300.f, 300.f), omega(-5.f, 5.f), rcs(-10.f, 20.f);
    targets_.reserve(n_tracks);
    for (std::size_t i = 0; i < n_tracks; ++i)
        targets_.push_back({range(rng), az(rng), el(rng), vel(rng), omega(rng), rcs(rng)});
}

TrackFrame FakeTrackGenerator::next(std::uint64_t timestamp_us, double dt_seconds) {
    TrackFrame frame;
    frame.seq = seq_++;
    frame.timestamp_us = timestamp_us;
    frame.tracks.reserve(targets_.size());
    const float dt = static_cast<float>(dt_seconds);
    for (std::size_t i = 0; i < targets_.size(); ++i) {
        Target& t = targets_[i];
        t.range += t.velocity * dt;
        if (t.range < 500.f || t.range > 30000.f) t.velocity = -t.velocity;  // bounce
        t.azimuth = std::fmod(t.azimuth + t.omega * dt + 360.f, 360.f);
        frame.tracks.push_back({static_cast<std::uint32_t>(i + 1), t.range, t.azimuth, t.elevation,
                                t.velocity, t.rcs});
    }
    return frame;
}

// ---- Serialization ----------------------------------------------------------

std::vector<std::uint8_t> serialize_frame(const TrackFrame& frame) {
    std::vector<std::uint8_t> out;
    out.reserve(kFrameHeaderWireSize + frame.tracks.size() * kTrackWireSize);
    put_u32(out, frame.seq);
    put_u64(out, frame.timestamp_us);
    put_u32(out, static_cast<std::uint32_t>(frame.tracks.size()));
    for (const Track& t : frame.tracks) {
        put_u32(out, t.id);
        put_f32(out, t.range_m);
        put_f32(out, t.azimuth_deg);
        put_f32(out, t.elevation_deg);
        put_f32(out, t.velocity_mps);
        put_f32(out, t.rcs_dbsm);
    }
    return out;
}

std::optional<TrackFrame> deserialize_frame(const std::uint8_t* data, std::size_t len) {
    if (len < kFrameHeaderWireSize) return std::nullopt;
    TrackFrame frame;
    frame.seq = get_u32(data);
    frame.timestamp_us = get_u64(data + 4);
    const std::uint32_t count = get_u32(data + 12);
    if (len != kFrameHeaderWireSize + static_cast<std::size_t>(count) * kTrackWireSize) return std::nullopt;
    frame.tracks.reserve(count);
    const std::uint8_t* p = data + kFrameHeaderWireSize;
    for (std::uint32_t i = 0; i < count; ++i, p += kTrackWireSize)
        frame.tracks.push_back({get_u32(p), get_f32(p + 4), get_f32(p + 8), get_f32(p + 12), get_f32(p + 16),
                                get_f32(p + 20)});
    return frame;
}

// ---- Fragmentation ----------------------------------------------------------

std::size_t fragment_count_for(std::size_t payload_size, std::size_t max_datagram) {
    const std::size_t chunk = max_datagram - kFragmentHeaderSize;
    return std::max<std::size_t>(1, (payload_size + chunk - 1) / chunk);
}

std::vector<std::vector<std::uint8_t>> fragment_payload(const std::vector<std::uint8_t>& payload,
                                                        std::uint32_t frame_seq, std::size_t max_datagram) {
    const std::size_t chunk = max_datagram - kFragmentHeaderSize;
    const std::size_t count = fragment_count_for(payload.size(), max_datagram);
    std::vector<std::vector<std::uint8_t>> out;
    out.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        const std::size_t offset = i * chunk;
        const std::size_t n = std::min(chunk, payload.size() - std::min(offset, payload.size()));
        std::vector<std::uint8_t> d;
        d.reserve(kFragmentHeaderSize + n);
        put_u32(d, kFragmentMagic);
        put_u32(d, frame_seq);
        put_u16(d, static_cast<std::uint16_t>(i));
        put_u16(d, static_cast<std::uint16_t>(count));
        put_u32(d, static_cast<std::uint32_t>(payload.size()));
        put_u32(d, static_cast<std::uint32_t>(offset));
        d.insert(d.end(), payload.begin() + static_cast<std::ptrdiff_t>(offset),
                 payload.begin() + static_cast<std::ptrdiff_t>(offset + n));
        out.push_back(std::move(d));
    }
    return out;
}

std::optional<FragmentHeader> parse_fragment(const std::uint8_t* data, std::size_t len) {
    if (len < kFragmentHeaderSize || get_u32(data) != kFragmentMagic) return std::nullopt;
    FragmentHeader h;
    h.frame_seq = get_u32(data + 4);
    h.index = get_u16(data + 8);
    h.count = get_u16(data + 10);
    h.total_size = get_u32(data + 12);
    h.offset = get_u32(data + 16);
    h.payload_len = len - kFragmentHeaderSize;
    if (h.count == 0 || h.index >= h.count) return std::nullopt;
    if (static_cast<std::uint64_t>(h.offset) + h.payload_len > h.total_size) return std::nullopt;
    return h;
}

PreparedFrame prepare_frame(const TrackFrame& frame, std::size_t max_datagram) {
    PreparedFrame p;
    p.seq = frame.seq;
    p.fragments = fragment_payload(serialize_frame(frame), frame.seq, max_datagram);
    return p;
}

// ---- Reassembly -------------------------------------------------------------

std::optional<std::vector<std::uint8_t>> FrameReassembler::add(const std::uint8_t* data, std::size_t len) {
    const auto h = parse_fragment(data, len);
    if (!h) {
        ++invalid_;
        return std::nullopt;
    }
    ++fragments_;

    auto it = pending_.find(h->frame_seq);
    if (it == pending_.end()) {
        if (pending_.size() >= max_pending_) {  // give up on the oldest incomplete frame
            pending_.erase(pending_.begin());
            ++abandoned_;
        }
        Partial p;
        p.data.resize(h->total_size);
        p.have.assign(h->count, false);
        it = pending_.emplace(h->frame_seq, std::move(p)).first;
    }
    Partial& p = it->second;
    if (p.have.size() != h->count || p.data.size() != h->total_size) {
        ++invalid_;
        return std::nullopt;
    }
    if (p.have[h->index]) {
        ++duplicates_;
        return std::nullopt;
    }
    std::memcpy(p.data.data() + h->offset, data + kFragmentHeaderSize, h->payload_len);
    p.have[h->index] = true;
    if (++p.received < h->count) return std::nullopt;

    std::vector<std::uint8_t> done = std::move(p.data);
    pending_.erase(it);
    ++completed_;
    return done;
}

}  // namespace radar
