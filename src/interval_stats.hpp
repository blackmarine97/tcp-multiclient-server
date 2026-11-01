#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>

namespace radar {

// Summary of measured time intervals against a target period. Not thread-safe.
class IntervalStats {
public:
    explicit IntervalStats(std::int64_t target_us = 0) : target_us_(target_us) {}

    void add(std::int64_t interval_us) {
        min_us_ = count_ == 0 ? interval_us : std::min(min_us_, interval_us);
        max_us_ = count_ == 0 ? interval_us : std::max(max_us_, interval_us);
        ++count_;
        sum_us_ += interval_us;
        deviation_sum_us_ += std::llabs(interval_us - target_us_);
    }

    void reset() { *this = IntervalStats(target_us_); }

    std::uint64_t count() const { return count_; }
    std::int64_t min_us() const { return min_us_; }
    std::int64_t max_us() const { return max_us_; }
    double mean_us() const { return count_ ? static_cast<double>(sum_us_) / static_cast<double>(count_) : 0.0; }
    // Mean absolute difference from the target period.
    double jitter_us() const {
        return count_ ? static_cast<double>(deviation_sum_us_) / static_cast<double>(count_) : 0.0;
    }

private:
    std::int64_t target_us_;
    std::uint64_t count_ = 0;
    std::int64_t sum_us_ = 0;
    std::int64_t deviation_sum_us_ = 0;
    std::int64_t min_us_ = 0;
    std::int64_t max_us_ = 0;
};

}  // namespace radar
