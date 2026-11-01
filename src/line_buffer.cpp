#include "line_buffer.hpp"

namespace radar {

bool LineBuffer::append(const char* data, std::size_t len) {
    for (std::size_t i = 0; i < len; ++i) {
        if (data[i] == '\n') {
            current_len_ = 0;
        } else if (++current_len_ > max_line_) {
            overflow_ = true;
        }
    }
    buf_.append(data, len);
    return !overflow_;
}

std::optional<std::string> LineBuffer::next_line() {
    const auto pos = buf_.find('\n');
    if (pos == std::string::npos || pos > max_line_) return std::nullopt;
    std::string line = buf_.substr(0, pos);
    buf_.erase(0, pos + 1);
    if (!line.empty() && line.back() == '\r') line.pop_back();
    return line;
}

bool LineBuffer::has_line() const {
    const auto pos = buf_.find('\n');
    return pos != std::string::npos && pos <= max_line_;
}

void LineBuffer::clear() {
    buf_.clear();
    current_len_ = 0;
    overflow_ = false;
}

}  // namespace radar
