#pragma once

#include <algorithm>
#include <chrono>

namespace radar {

using Clock = std::chrono::steady_clock;
using Millis = std::chrono::milliseconds;

// Zombie rule (application level): a client that has sent no bytes at all for
// `zombie_timeout` is dead, even if the kernel still thinks the TCP connection is up.
// The server sends "HB <n>" every `heartbeat_interval`, so a healthy client always
// has something to answer (PONG) well inside the timeout.
enum class Liveness { Alive, Zombie };

inline Liveness judge_liveness(Clock::time_point last_rx, Clock::time_point now, Millis zombie_timeout) {
    return (now - last_rx) >= zombie_timeout ? Liveness::Zombie : Liveness::Alive;
}

inline bool heartbeat_due(Clock::time_point last_hb, Clock::time_point now, Millis heartbeat_interval) {
    return (now - last_hb) >= heartbeat_interval;
}

// Earliest moment at which the IO loop must look at this client again.
inline Clock::time_point next_liveness_deadline(Clock::time_point last_rx, Clock::time_point last_hb,
                                                Millis heartbeat_interval, Millis zombie_timeout) {
    return std::min(last_hb + heartbeat_interval, last_rx + zombie_timeout);
}

}  // namespace radar
