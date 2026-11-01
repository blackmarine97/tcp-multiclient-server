#pragma once

#include <algorithm>
#include <chrono>

namespace radar {

// Exponential retry delay: min, 2*min, 4*min, ... capped at max.
// failures() counts delays handed out since the last reset(), i.e. consecutive
// attempts that did not reach a working session.
class ReconnectBackoff {
public:
    ReconnectBackoff(std::chrono::milliseconds min_delay, std::chrono::milliseconds max_delay)
        : min_(min_delay), max_(std::max(min_delay, max_delay)), next_(min_) {}

    std::chrono::milliseconds next_delay() {
        const auto delay = next_;
        next_ = std::min(next_ * 2, max_);
        ++failures_;
        return delay;
    }

    void reset() {
        next_ = min_;
        failures_ = 0;
    }

    int failures() const { return failures_; }

private:
    std::chrono::milliseconds min_, max_, next_;
    int failures_ = 0;
};

}  // namespace radar
