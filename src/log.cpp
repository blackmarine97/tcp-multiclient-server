#include "log.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace radar {

namespace {

std::atomic<int> g_level{static_cast<int>(LogLevel::Info)};
std::mutex g_write_mtx;

const char* level_name(LogLevel level) {
    switch (level) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info: return "INFO ";
        case LogLevel::Warn: return "WARN ";
        case LogLevel::Error: return "ERROR";
        case LogLevel::Off: return "OFF  ";
    }
    return "?";
}

}  // namespace

void set_log_level(LogLevel level) { g_level.store(static_cast<int>(level)); }

LogLevel log_level() { return static_cast<LogLevel>(g_level.load()); }

bool log_enabled(LogLevel level) {
    return level != LogLevel::Off && static_cast<int>(level) >= g_level.load();
}

std::optional<LogLevel> parse_log_level(std::string_view text) {
    if (text == "debug") return LogLevel::Debug;
    if (text == "info") return LogLevel::Info;
    if (text == "warn") return LogLevel::Warn;
    if (text == "error") return LogLevel::Error;
    if (text == "off") return LogLevel::Off;
    return std::nullopt;
}

void log_write(LogLevel level, const std::string& message) {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const std::time_t t = system_clock::to_time_t(now);
    const int ms = static_cast<int>(duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000);
    std::tm tmv{};
    localtime_r(&t, &tmv);
    char ts[16];
    std::strftime(ts, sizeof ts, "%H:%M:%S", &tmv);

    std::lock_guard<std::mutex> lk(g_write_mtx);
    std::fprintf(stderr, "%s.%03d [%s] %s\n", ts, ms, level_name(level), message.c_str());
}

}  // namespace radar
