#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <istream>
#include <optional>
#include <string>

#include "bounded_queue.hpp"
#include "log.hpp"

namespace radar {

struct Config {
    std::string bind_address = "0.0.0.0";
    std::uint16_t tcp_port = 8088;  // 0 = pick an ephemeral port (tests)
    std::size_t max_clients = 3;

    std::chrono::milliseconds frame_interval{35};
    std::chrono::milliseconds heartbeat_interval{1000};
    std::chrono::milliseconds zombie_timeout{3500};
    std::chrono::milliseconds stats_interval{5000};  // periodic stats log; 0 = off

    std::size_t queue_capacity = 8;  // frames per client
    OverflowPolicy queue_overflow = OverflowPolicy::DropOldest;

    std::size_t tracks_per_frame = 64;
    std::size_t max_datagram_bytes = 512;  // UDP payload per fragment, header included
    std::size_t max_line_bytes = 256;

    LogLevel log_level = LogLevel::Info;
};

// Format: "key = value" per line, '#' starts a comment. Unknown keys are errors.
std::optional<Config> parse_config(std::istream& in, std::string* error);
std::optional<Config> load_config_file(const std::string& path, std::string* error);

// Cross-field checks. Returns an empty string when the config is usable.
std::string validate_config(const Config& cfg);

}  // namespace radar
