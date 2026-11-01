#pragma once

// Blocking-ish TCP+UDP client for integration tests. Each instance must be used
// from one thread at a time.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <thread>

#include "line_buffer.hpp"
#include "stream_stats.hpp"
#include "track_frame.hpp"

namespace radar::test {

using namespace std::chrono_literals;
using Ms = std::chrono::milliseconds;
using SteadyClock = std::chrono::steady_clock;

class TestClient {
public:
    TestClient() : in_(1 << 20) {}
    ~TestClient() { close(); }
    TestClient(const TestClient&) = delete;
    TestClient& operator=(const TestClient&) = delete;

    // rcvbuf > 0 shrinks the receive buffer (must be set before connecting).
    bool connect(std::uint16_t port, int rcvbuf = 0) {
        tcp_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (rcvbuf > 0) ::setsockopt(tcp_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof rcvbuf);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port = htons(port);
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        return tcp_ >= 0 && ::connect(tcp_, reinterpret_cast<sockaddr*>(&a), sizeof a) == 0;
    }

    std::uint16_t open_udp() {
        udp_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        int rcvbuf = 1 << 20;
        ::setsockopt(udp_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof rcvbuf);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ::bind(udp_, reinterpret_cast<sockaddr*>(&a), sizeof a);
        socklen_t len = sizeof a;
        ::getsockname(udp_, reinterpret_cast<sockaddr*>(&a), &len);
        return ntohs(a.sin_port);
    }

    bool send_line(const std::string& line) { return send_raw(line + "\n"); }

    bool send_raw(const std::string& d) {
        std::size_t off = 0;
        while (off < d.size()) {
            const ssize_t n = ::send(tcp_, d.data() + off, d.size() - off, MSG_NOSIGNAL);
            if (n <= 0) return false;
            off += static_cast<std::size_t>(n);
        }
        return true;
    }

    // Next non-heartbeat line, or nullopt on timeout / connection closed.
    std::optional<std::string> read_reply(Ms timeout = 2000ms) {
        const auto deadline = SteadyClock::now() + timeout;
        for (;;) {
            while (!lines_.empty()) {
                std::string l = std::move(lines_.front());
                lines_.pop_front();
                if (l.rfind("HB ", 0) == 0) continue;
                return l;
            }
            const auto now = SteadyClock::now();
            if (eof_ || now >= deadline) return std::nullopt;
            pump_once(std::chrono::ceil<Ms>(deadline - now));
        }
    }

    // True once the server closed the TCP connection.
    bool wait_eof(Ms timeout) {
        const auto deadline = SteadyClock::now() + timeout;
        while (!eof_) {
            const auto now = SteadyClock::now();
            if (now >= deadline) return false;
            pump_once(std::chrono::ceil<Ms>(deadline - now));
        }
        return true;
    }

    // Services TCP (heartbeats) and UDP (fragments) for `duration`.
    void pump(Ms duration) {
        const auto deadline = SteadyClock::now() + duration;
        for (auto now = SteadyClock::now(); now < deadline; now = SteadyClock::now())
            pump_once(std::chrono::ceil<Ms>(deadline - now));
    }

    void close() {
        if (tcp_ >= 0) ::close(tcp_);
        if (udp_ >= 0) ::close(udp_);
        tcp_ = udp_ = -1;
    }

    const FrameReassembler& udp_stats() const { return reasm_; }
    const StreamStats& stream_stats() const { return stream_; }
    std::uint64_t heartbeats() const { return heartbeats_; }
    bool eof() const { return eof_; }
    int tcp_fd() const { return tcp_; }
    bool auto_pong = true;

private:
    void pump_once(Ms timeout) {
        pollfd p[2];
        nfds_t n = 0;
        int tcp_idx = -1, udp_idx = -1;
        if (tcp_ >= 0 && !eof_) {
            p[n] = {tcp_, POLLIN, 0};
            tcp_idx = static_cast<int>(n++);
        }
        if (udp_ >= 0) {
            p[n] = {udp_, POLLIN, 0};
            udp_idx = static_cast<int>(n++);
        }
        if (n == 0) {
            std::this_thread::sleep_for(timeout);
            return;
        }
        if (::poll(p, n, static_cast<int>(std::max<long long>(1, timeout.count()))) <= 0) return;
        if (tcp_idx >= 0 && p[tcp_idx].revents) read_tcp();
        if (udp_idx >= 0 && (p[udp_idx].revents & POLLIN)) read_udp();
    }

    void read_tcp() {
        char buf[4096];
        const ssize_t n = ::recv(tcp_, buf, sizeof buf, MSG_DONTWAIT);
        if (n == 0 || (n < 0 && errno != EAGAIN && errno != EINTR)) {
            eof_ = true;
            return;
        }
        if (n < 0) return;
        in_.append(buf, static_cast<std::size_t>(n));
        while (auto l = in_.next_line()) {
            if (l->rfind("HB ", 0) == 0) {
                ++heartbeats_;
                if (auto_pong) send_line("PONG " + l->substr(3));
            }
            lines_.push_back(std::move(*l));
        }
    }

    void read_udp() {
        std::uint8_t buf[65536];
        for (;;) {
            const ssize_t n = ::recv(udp_, buf, sizeof buf, 0);
            if (n < 0) return;
            const auto payload = reasm_.add(buf, static_cast<std::size_t>(n));
            if (!payload) continue;
            const auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
            if (const auto frame = deserialize_frame(payload->data(), payload->size()))
                stream_.on_frame(frame->seq, frame->timestamp_us, static_cast<std::uint64_t>(now_us));
        }
    }

    int tcp_ = -1, udp_ = -1;
    LineBuffer in_;
    std::deque<std::string> lines_;
    FrameReassembler reasm_{64};
    StreamStats stream_;
    std::uint64_t heartbeats_ = 0;
    bool eof_ = false;
};

// Polls `pred` every 5 ms until it holds or `timeout` expires.
template <typename Pred>
bool wait_until(Pred pred, Ms timeout) {
    const auto deadline = SteadyClock::now() + timeout;
    while (SteadyClock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return pred();
}

// Extracts "key=value" from a STATUS line.
inline std::uint64_t status_field(const std::string& line, const std::string& key) {
    const auto pos = line.find(" " + key + "=");
    if (pos == std::string::npos) return 0;
    return std::stoull(line.substr(pos + key.size() + 2));
}

}  // namespace radar::test
