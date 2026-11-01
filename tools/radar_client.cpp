// radar_client - demo / test client.
//
//   radar_client [--host 127.0.0.1] [--port 8088] [--udp-port 0] [--duration-ms 0]
//                [--no-pong] [--no-start] [--quiet]
//                [--reconnect-min-ms 1000] [--reconnect-max-ms 8000]
//                [--max-retries 0]
//
// Connects, REGISTERs a UDP port, STARTs streaming, answers heartbeats and
// counts received fragments / reassembled frames. --duration-ms 0 runs until Ctrl+C.
//
// A lost connection, a refused connection, ERR FULL or a silent server do not end
// the program: it retries with exponential backoff and sends REGISTER / START again.
// Only the client retries; the server treats every reconnect as a brand-new client.
// --max-retries N gives up after N consecutive failures (0 = never).
//
// Exit codes: 0 ok, 1 bad arguments or local setup error, 3 gave up reconnecting.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <random>
#include <string>

#include "backoff.hpp"
#include "line_buffer.hpp"
#include "socket_util.hpp"
#include "track_frame.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using Millis = std::chrono::milliseconds;

// A server that accepts but never says WELCOME (e.g. frozen: the kernel still
// completes the handshake) counts as lost after this long.
constexpr Millis kWelcomeTimeout{5000};

volatile sig_atomic_t g_interrupted = 0;
void on_sigint(int) { g_interrupted = 1; }

struct Options {
    std::string host = "127.0.0.1";
    int port = 8088;
    int udp_port = 0;
    long duration_ms = 0;
    bool pong = true;
    bool start = true;
    bool quiet = false;
    long reconnect_min_ms = 1000;
    long reconnect_max_ms = 8000;
    int max_retries = 0;
};

// Survives across reconnects, so the summary covers the whole run.
struct Totals {
    radar::FrameReassembler reasm{32};
    std::uint64_t heartbeats = 0;
    std::uint64_t sessions = 0;
    std::uint64_t rejected_full = 0;
    Clock::time_point last_report = Clock::now();
};

enum class SessionEnd {
    Finished,       // Ctrl+C or --duration-ms: said goodbye
    Lost,           // server closed the connection or went silent
    Full,           // ERR FULL
    ConnectFailed,  // no TCP connection at all
};

bool send_line(int fd, const std::string& line) {
    const std::string data = line + "\n";
    std::size_t off = 0;
    while (off < data.size()) {
        const ssize_t n = ::send(fd, data.data() + off, data.size() - off, MSG_NOSIGNAL);
        if (n <= 0) return false;
        off += static_cast<std::size_t>(n);
    }
    return true;
}

bool starts_with(const std::string& s, const char* prefix) { return s.rfind(prefix, 0) == 0; }

// Value of "key=<number>" in a server line, if present.
std::optional<long> line_field(const std::string& line, const std::string& key) {
    const auto pos = line.find(" " + key + "=");
    if (pos == std::string::npos) return std::nullopt;
    const long v = std::atol(line.c_str() + pos + key.size() + 2);
    return v > 0 ? std::optional<long>(v) : std::nullopt;
}

void say_goodbye(int tcp, radar::LineBuffer& lines, bool quiet) {
    send_line(tcp, "STOP");
    send_line(tcp, "QUIT");
    ::shutdown(tcp, SHUT_WR);
    pollfd p{tcp, POLLIN, 0};
    char buf[1024];
    while (::poll(&p, 1, 1000) > 0) {
        const ssize_t n = ::recv(tcp, buf, sizeof buf, 0);
        if (n <= 0) break;
        lines.append(buf, static_cast<std::size_t>(n));
        while (auto line = lines.next_line())
            if (!quiet && !starts_with(*line, "HB ")) std::cout << "<< " << *line << std::endl;
    }
}

// One TCP connection, from connect() until it ends for whatever reason.
SessionEnd run_session(const Options& opt, const sockaddr_in& server, int udp, int udp_port,
                       std::optional<Clock::time_point> deadline, radar::ReconnectBackoff& backoff,
                       Totals& totals) {
    radar::UniqueFd tcp(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    if (!tcp.valid() || ::connect(tcp.get(), reinterpret_cast<const sockaddr*>(&server), sizeof server) != 0) {
        if (!g_interrupted && !opt.quiet) std::perror("connect");
        return SessionEnd::ConnectFailed;
    }

    radar::LineBuffer lines(4096);
    bool welcomed = false;
    Millis silence_limit = kWelcomeTimeout;
    auto last_rx = Clock::now();

    while (!g_interrupted) {
        const auto now = Clock::now();
        if (deadline && now >= *deadline) break;
        if (now - last_rx >= silence_limit) {
            if (!opt.quiet) std::cout << "server silent for " << silence_limit.count() << " ms" << std::endl;
            return SessionEnd::Lost;
        }

        pollfd p[2] = {{tcp.get(), POLLIN, 0}, {udp, POLLIN, 0}};
        const int rc = ::poll(p, 2, 200);
        if (rc < 0 && errno != EINTR) return SessionEnd::Lost;

        if (rc > 0 && p[0].revents) {
            char buf[4096];
            const ssize_t n = ::recv(tcp.get(), buf, sizeof buf, 0);
            if (n <= 0) {
                if (!opt.quiet) std::cout << "server closed the connection" << std::endl;
                return SessionEnd::Lost;
            }
            last_rx = Clock::now();
            lines.append(buf, static_cast<std::size_t>(n));
            while (auto line = lines.next_line()) {
                if (starts_with(*line, "HB ")) {
                    ++totals.heartbeats;
                    if (opt.pong) send_line(tcp.get(), "PONG " + line->substr(3));
                    continue;
                }
                if (!opt.quiet) std::cout << "<< " << *line << std::endl;
                if (*line == "ERR FULL") return SessionEnd::Full;
                if (starts_with(*line, "WELCOME") && !welcomed) {
                    welcomed = true;
                    ++totals.sessions;
                    backoff.reset();
                    if (auto limit = line_field(*line, "timeout_ms")) silence_limit = Millis(*limit);
                    send_line(tcp.get(), "REGISTER " + std::to_string(udp_port));
                    if (opt.start) send_line(tcp.get(), "START");
                }
                if (*line == "OK STREAMING") std::cout << "READY udp_port=" << udp_port << std::endl;
            }
        }
        if (rc > 0 && (p[1].revents & POLLIN)) {
            std::uint8_t buf[65536];
            for (;;) {
                const ssize_t n = ::recv(udp, buf, sizeof buf, 0);
                if (n < 0) break;
                totals.reasm.add(buf, static_cast<std::size_t>(n));
            }
        }
        if (!opt.quiet && Clock::now() - totals.last_report >= std::chrono::seconds(1)) {
            totals.last_report = Clock::now();
            std::cout << "STATS fragments=" << totals.reasm.fragments()
                      << " frames=" << totals.reasm.frames_completed()
                      << " incomplete=" << totals.reasm.frames_abandoned()
                      << " heartbeats=" << totals.heartbeats << std::endl;
        }
    }

    say_goodbye(tcp.get(), lines, opt.quiet);
    return SessionEnd::Finished;
}

// Sleeps in short slices so Ctrl+C and --duration-ms interrupt a long backoff promptly.
void interruptible_sleep(Millis delay, std::optional<Clock::time_point> deadline) {
    const auto until = Clock::now() + delay;
    while (!g_interrupted) {
        const auto now = Clock::now();
        if (now >= until || (deadline && now >= *deadline)) return;
        const auto left = std::chrono::duration_cast<Millis>(until - now).count();
        ::poll(nullptr, 0, static_cast<int>(std::min<long long>(left + 1, 100)));
    }
}

void print_summary(const Totals& t) {
    std::cout << "SUMMARY fragments=" << t.reasm.fragments() << " frames=" << t.reasm.frames_completed()
              << " incomplete=" << t.reasm.frames_abandoned() << " invalid=" << t.reasm.invalid()
              << " heartbeats=" << t.heartbeats << " sessions=" << t.sessions
              << " rejected_full=" << t.rejected_full << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&] { return i + 1 < argc ? std::string(argv[++i]) : std::string(); };
        if (a == "--host") opt.host = next();
        else if (a == "--port") opt.port = std::atoi(next().c_str());
        else if (a == "--udp-port") opt.udp_port = std::atoi(next().c_str());
        else if (a == "--duration-ms") opt.duration_ms = std::atol(next().c_str());
        else if (a == "--no-pong") opt.pong = false;
        else if (a == "--no-start") opt.start = false;
        else if (a == "--quiet") opt.quiet = true;
        else if (a == "--reconnect-min-ms") opt.reconnect_min_ms = std::max(1L, std::atol(next().c_str()));
        else if (a == "--reconnect-max-ms") opt.reconnect_max_ms = std::max(1L, std::atol(next().c_str()));
        else if (a == "--max-retries") opt.max_retries = std::atoi(next().c_str());
        else {
            std::cerr << "usage: " << argv[0]
                      << " [--host H] [--port P] [--udp-port U] [--duration-ms N] [--no-pong] [--no-start]"
                         " [--quiet] [--reconnect-min-ms N] [--reconnect-max-ms N]"
                         " [--max-retries N]\n";
            return 1;
        }
    }

    struct sigaction sa {};
    sa.sa_handler = on_sigint;  // no SA_RESTART: poll() and connect() return EINTR
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    // The UDP socket outlives TCP sessions, so the port stays the same across reconnects.
    const radar::UniqueFd udp(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    sockaddr_in uaddr{};
    uaddr.sin_family = AF_INET;
    uaddr.sin_port = htons(static_cast<std::uint16_t>(opt.udp_port));
    uaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (!udp.valid() || ::bind(udp.get(), reinterpret_cast<sockaddr*>(&uaddr), sizeof uaddr) != 0) {
        std::perror("udp bind");
        return 1;
    }
    socklen_t ulen = sizeof uaddr;
    ::getsockname(udp.get(), reinterpret_cast<sockaddr*>(&uaddr), &ulen);
    const int my_udp_port = ntohs(uaddr.sin_port);

    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(static_cast<std::uint16_t>(opt.port));
    if (::inet_pton(AF_INET, opt.host.c_str(), &server.sin_addr) != 1) {
        std::cerr << "bad host " << opt.host << "\n";
        return 1;
    }

    std::optional<Clock::time_point> deadline;
    if (opt.duration_ms > 0) deadline = Clock::now() + Millis(opt.duration_ms);

    Totals totals;
    radar::ReconnectBackoff backoff{Millis(opt.reconnect_min_ms), Millis(opt.reconnect_max_ms)};
    std::mt19937 rng(static_cast<std::uint32_t>(::getpid()));

    for (;;) {
        const SessionEnd end = run_session(opt, server, udp.get(), my_udp_port, deadline, backoff, totals);
        if (end == SessionEnd::Full) ++totals.rejected_full;
        if (end == SessionEnd::Finished) break;

        if (g_interrupted || (deadline && Clock::now() >= *deadline)) break;
        if (opt.max_retries > 0 && backoff.failures() >= opt.max_retries) {
            std::cout << "GAVE UP after " << backoff.failures() << " retries" << std::endl;
            print_summary(totals);
            return 3;
        }

        // Fragments of the interrupted frame will never complete; a restarted server
        // also reuses frame numbers, so old pieces must not mix with new ones.
        totals.reasm.discard_pending();

        // Up to 25% jitter, so clients cut off together do not all retry together.
        auto delay = backoff.next_delay();
        delay += Millis(std::uniform_int_distribution<long>(0, delay.count() / 4)(rng));
        if (!opt.quiet) std::cout << "RECONNECT in " << delay.count() << " ms" << std::endl;
        interruptible_sleep(delay, deadline);
        if (g_interrupted || (deadline && Clock::now() >= *deadline)) break;
    }

    print_summary(totals);
    return 0;
}
