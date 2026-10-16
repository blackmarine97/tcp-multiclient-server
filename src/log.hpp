#pragma once

#include <optional>
#include <sstream>
#include <string>
#include <string_view>

namespace radar {

enum class LogLevel { Debug = 0, Info, Warn, Error, Off };

void set_log_level(LogLevel level);
LogLevel log_level();
bool log_enabled(LogLevel level);
std::optional<LogLevel> parse_log_level(std::string_view text);
void log_write(LogLevel level, const std::string& message);

// Thread-safe; formats only when the level is enabled.
template <typename... Args>
void log_msg(LogLevel level, const Args&... args) {
    if (!log_enabled(level)) return;
    std::ostringstream os;
    (os << ... << args);
    log_write(level, os.str());
}

}  // namespace radar
