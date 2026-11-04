#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>

namespace radar {

// Receiver-side view of the frame stream, fed with each completely reassembled frame.
//
// lost() counts gaps in the frame numbers, whatever caused them: UDP loss, a frame
// that never completed, or a frame the server dropped from a full queue.
// Latency compares the server's timestamp with the receiver's clock, so it is only
// meaningful when both clocks agree (same machine, or synchronized by NTP/PTP).
class StreamStats {
public:
    void on_frame(std::uint32_t seq, std::uint64_t sent_us, std::uint64_t received_us) {
        ++received_;
        // Unsigned subtraction read as signed keeps the order right when seq wraps around.
        const auto ahead = last_seq_ ? static_cast<std::int32_t>(seq - *last_seq_) : 1;
        if (!last_seq_) {
            last_seq_ = seq;
        } else if (ahead > 0) {
            lost_ += static_cast<std::uint32_t>(ahead) - 1;
            last_seq_ = seq;
        } else {  // arrived after a newer frame: it was late, not lost
            ++reordered_;
            if (lost_ > 0) --lost_;
        }

        const auto latency = static_cast<std::int64_t>(received_us) - static_cast<std::int64_t>(sent_us);
        latency_min_us_ = received_ == 1 ? latency : std::min(latency_min_us_, latency);
        latency_max_us_ = received_ == 1 ? latency : std::max(latency_max_us_, latency);
        latency_sum_us_ += latency;
    }

    // After a reconnect the next frame is a new baseline: frames sent while nobody was
    // connected are not loss, and a restarted server numbers its frames from 0 again.
    void new_session() { last_seq_.reset(); }

    std::uint64_t received() const { return received_; }
    std::uint64_t lost() const { return lost_; }
    std::uint64_t reordered() const { return reordered_; }
    double loss_percent() const {
        const auto total = received_ + lost_;
        return total ? 100.0 * static_cast<double>(lost_) / static_cast<double>(total) : 0.0;
    }
    std::int64_t latency_min_us() const { return latency_min_us_; }
    std::int64_t latency_max_us() const { return latency_max_us_; }
    double latency_mean_us() const {
        return received_ ? static_cast<double>(latency_sum_us_) / static_cast<double>(received_) : 0.0;
    }

private:
    std::optional<std::uint32_t> last_seq_;
    std::uint64_t received_ = 0;
    std::uint64_t lost_ = 0;
    std::uint64_t reordered_ = 0;
    std::int64_t latency_sum_us_ = 0;
    std::int64_t latency_min_us_ = 0;
    std::int64_t latency_max_us_ = 0;
};

}  // namespace radar
