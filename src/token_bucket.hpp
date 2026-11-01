#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>

#include "liveness.hpp"

namespace radar {

// Allows `rate` events per second on average, with bursts of up to one second's worth.
// A rate of 0 means unlimited. Not thread-safe.
class TokenBucket {
public:
    explicit TokenBucket(std::size_t rate = 0) : rate_(static_cast<double>(rate)), tokens_(rate_) {}

    void reset(Clock::time_point now) {
        tokens_ = rate_;
        last_ = now;
    }

    bool try_take(Clock::time_point now) {
        if (rate_ <= 0.0) return true;
        const std::chrono::duration<double> elapsed = now - last_;
        tokens_ = std::min(rate_, tokens_ + elapsed.count() * rate_);
        last_ = now;
        if (tokens_ < 1.0) return false;
        tokens_ -= 1.0;
        return true;
    }

    // When try_take() can succeed again.
    Clock::time_point next_available(Clock::time_point now) const {
        if (rate_ <= 0.0 || tokens_ >= 1.0) return now;
        const std::chrono::duration<double> wait((1.0 - tokens_) / rate_);
        return now + std::chrono::ceil<Clock::duration>(wait);
    }

private:
    double rate_;
    double tokens_;
    Clock::time_point last_{};
};

}  // namespace radar
