#include "config.hpp"

#include <arpa/inet.h>

#include <cctype>
#include <fstream>

#include "track_frame.hpp"

namespace radar {

namespace {

std::string trim(const std::string& s) {
    std::size_t b = 0, e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

bool parse_uint(const std::string& v, std::uint64_t min, std::uint64_t max, std::uint64_t& out) {
    if (v.empty() || v.size() > 12) return false;
    std::uint64_t n = 0;
    for (char c : v) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
        n = n * 10 + static_cast<std::uint64_t>(c - '0');
    }
    if (n < min || n > max) return false;
    out = n;
    return true;
}

}  // namespace

std::optional<Config> parse_config(std::istream& in, std::string* error) {
    Config cfg;
    std::string raw;
    int line_no = 0;

    auto fail = [&](const std::string& msg) -> std::optional<Config> {
        if (error) *error = "line " + std::to_string(line_no) + ": " + msg;
        return std::nullopt;
    };

    while (std::getline(in, raw)) {
        ++line_no;
        if (auto hash = raw.find('#'); hash != std::string::npos) raw.erase(hash);
        const std::string line = trim(raw);
        if (line.empty()) continue;

        const auto eq = line.find('=');
        if (eq == std::string::npos) return fail("expected 'key = value'");
        const std::string key = trim(line.substr(0, eq));
        const std::string value = trim(line.substr(eq + 1));
        std::uint64_t n = 0;

        auto need_uint = [&](std::uint64_t min, std::uint64_t max) {
            return parse_uint(value, min, max, n);
        };
        auto range_msg = [&](std::uint64_t min, std::uint64_t max) {
            return key + " must be an integer in [" + std::to_string(min) + ", " + std::to_string(max) + "]";
        };

        if (key == "bind_address") {
            in_addr addr{};
            if (inet_pton(AF_INET, value.c_str(), &addr) != 1) return fail("bind_address must be an IPv4 address");
            cfg.bind_address = value;
        } else if (key == "tcp_port") {
            if (!need_uint(0, 65535)) return fail(range_msg(0, 65535));
            cfg.tcp_port = static_cast<std::uint16_t>(n);
        } else if (key == "max_clients") {
            if (!need_uint(1, 64)) return fail(range_msg(1, 64));
            cfg.max_clients = n;
        } else if (key == "frame_interval_ms") {
            if (!need_uint(1, 10000)) return fail(range_msg(1, 10000));
            cfg.frame_interval = std::chrono::milliseconds(n);
        } else if (key == "heartbeat_interval_ms") {
            if (!need_uint(10, 600000)) return fail(range_msg(10, 600000));
            cfg.heartbeat_interval = std::chrono::milliseconds(n);
        } else if (key == "zombie_timeout_ms") {
            if (!need_uint(20, 3600000)) return fail(range_msg(20, 3600000));
            cfg.zombie_timeout = std::chrono::milliseconds(n);
        } else if (key == "stats_interval_ms") {
            if (!need_uint(0, 3600000)) return fail(range_msg(0, 3600000));
            cfg.stats_interval = std::chrono::milliseconds(n);
        } else if (key == "queue_capacity") {
            if (!need_uint(1, 4096)) return fail(range_msg(1, 4096));
            cfg.queue_capacity = n;
        } else if (key == "queue_overflow") {
            if (value == "drop_oldest") cfg.queue_overflow = OverflowPolicy::DropOldest;
            else if (value == "drop_newest") cfg.queue_overflow = OverflowPolicy::DropNewest;
            else return fail("queue_overflow must be drop_oldest or drop_newest");
        } else if (key == "tracks_per_frame") {
            if (!need_uint(0, 10000)) return fail(range_msg(0, 10000));
            cfg.tracks_per_frame = n;
        } else if (key == "max_datagram_bytes") {
            if (!need_uint(kFragmentHeaderSize + 1, 65507)) return fail(range_msg(kFragmentHeaderSize + 1, 65507));
            cfg.max_datagram_bytes = n;
        } else if (key == "max_line_bytes") {
            if (!need_uint(16, 65536)) return fail(range_msg(16, 65536));
            cfg.max_line_bytes = n;
        } else if (key == "log_level") {
            auto lvl = parse_log_level(value);
            if (!lvl) return fail("log_level must be debug|info|warn|error|off");
            cfg.log_level = *lvl;
        } else {
            return fail("unknown key '" + key + "'");
        }
    }

    if (auto msg = validate_config(cfg); !msg.empty()) {
        if (error) *error = msg;
        return std::nullopt;
    }
    return cfg;
}

std::optional<Config> load_config_file(const std::string& path, std::string* error) {
    std::ifstream in(path);
    if (!in) {
        if (error) *error = "cannot open config file '" + path + "'";
        return std::nullopt;
    }
    std::string inner;
    auto cfg = parse_config(in, &inner);
    if (!cfg && error) *error = path + ": " + inner;
    return cfg;
}

std::string validate_config(const Config& cfg) {
    if (cfg.zombie_timeout <= cfg.heartbeat_interval)
        return "zombie_timeout_ms must be greater than heartbeat_interval_ms";
    if (cfg.max_datagram_bytes <= kFragmentHeaderSize)
        return "max_datagram_bytes must be larger than the fragment header";
    if (cfg.max_clients == 0) return "max_clients must be at least 1";
    if (cfg.queue_capacity == 0) return "queue_capacity must be at least 1";
    return {};
}

}  // namespace radar
