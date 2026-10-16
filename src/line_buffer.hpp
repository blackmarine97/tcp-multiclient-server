#pragma once

#include <cstddef>
#include <optional>
#include <string>

namespace radar {

// Accumulates bytes from a stream socket and yields complete '\n'-terminated lines.
// A trailing '\r' is stripped so "CMD\r\n" (telnet/nc -C) works too.
class LineBuffer {
public:
    explicit LineBuffer(std::size_t max_line_bytes = 256) : max_line_(max_line_bytes) {}

    // Returns false once any line (complete or still pending) exceeds the limit.
    bool append(const char* data, std::size_t len);

    std::optional<std::string> next_line();

    void clear();
    bool overflowed() const { return overflow_; }
    std::size_t pending_bytes() const { return buf_.size(); }

private:
    std::size_t max_line_;
    std::string buf_;
    std::size_t current_len_ = 0;  // bytes since the last '\n'
    bool overflow_ = false;
};

}  // namespace radar
