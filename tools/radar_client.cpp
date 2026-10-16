// radar_client - demo / test client.
//
//   radar_client [--host 127.0.0.1] [--port 8088] [--udp-port 0] [--duration-ms 0]
//                [--no-pong] [--no-start] [--quiet]
//
// Connects, REGISTERs a UDP port, STARTs streaming, answers heartbeats and
// counts received fragments / reassembled frames. --duration-ms 0 runs until
// Ctrl+C or until the server closes the connection.
// Exit codes: 0 ok, 1 error, 2 rejected with ERR FULL.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "line_buffer.hpp"
#include "track_frame.hpp"

namespace {

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
        else {
            std::cerr << "usage: " << argv[0]
                      << " [--host H] [--port P] [--udp-port U] [--duration-ms N] [--no-pong] [--no-start]"
                         " [--quiet]\n";
            return 1;
        }
    }

    struct sigaction sa {};
    sa.sa_handler = on_sigint;  // no SA_RESTART: poll() returns EINTR
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    // UDP receive socket
    const int udp = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
    sockaddr_in uaddr{};
    uaddr.sin_family = AF_INET;
    uaddr.sin_port = htons(static_cast<std::uint16_t>(opt.udp_port));
    uaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (udp < 0 || ::bind(udp, reinterpret_cast<sockaddr*>(&uaddr), sizeof uaddr) != 0) {
        std::perror("udp bind");
        return 1;
    }
    socklen_t ulen = sizeof uaddr;
    ::getsockname(udp, reinterpret_cast<sockaddr*>(&uaddr), &ulen);
    const int my_udp_port = ntohs(uaddr.sin_port);

    // TCP command connection
    const int tcp = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(static_cast<std::uint16_t>(opt.port));
    if (::inet_pton(AF_INET, opt.host.c_str(), &saddr.sin_addr) != 1) {
        std::cerr << "bad host " << opt.host << "\n";
        return 1;
    }
    if (::connect(tcp, reinterpret_cast<sockaddr*>(&saddr), sizeof saddr) != 0) {
        std::perror("connect");
        return 1;
    }

    radar::LineBuffer lines(4096);
    radar::FrameReassembler reasm(32);
    bool registered_sent = false;
    bool server_closed = false;
    int exit_code = 0;
    std::uint64_t heartbeats = 0;

    using Clock = std::chrono::steady_clock;
    const auto t0 = Clock::now();
    auto last_report = t0;

    while (!g_interrupted && !server_closed) {
        const auto now = Clock::now();
        if (opt.duration_ms > 0 && now - t0 >= std::chrono::milliseconds(opt.duration_ms)) break;

        pollfd p[2] = {{tcp, POLLIN, 0}, {udp, POLLIN, 0}};
        const int rc = ::poll(p, 2, 200);
        if (rc < 0 && errno != EINTR) break;

        if (rc > 0 && p[0].revents) {
            char buf[4096];
            const ssize_t n = ::recv(tcp, buf, sizeof buf, 0);
            if (n <= 0) {
                server_closed = true;
                if (!opt.quiet) std::cout << "server closed the connection" << std::endl;
            } else {
                lines.append(buf, static_cast<std::size_t>(n));
                while (auto line = lines.next_line()) {
                    if (starts_with(*line, "HB ")) {
                        ++heartbeats;
                        if (opt.pong) send_line(tcp, "PONG " + line->substr(3));
                        continue;
                    }
                    if (!opt.quiet) std::cout << "<< " << *line << std::endl;
                    if (*line == "ERR FULL") {
                        std::cout << "REJECTED: server is full" << std::endl;
                        return 2;
                    }
                    if (starts_with(*line, "WELCOME") && !registered_sent) {
                        registered_sent = true;
                        send_line(tcp, "REGISTER " + std::to_string(my_udp_port));
                        if (opt.start) send_line(tcp, "START");
                    }
                    if (*line == "OK STREAMING") std::cout << "READY udp_port=" << my_udp_port << std::endl;
                }
            }
        }
        if (rc > 0 && (p[1].revents & POLLIN)) {
            std::uint8_t buf[65536];
            for (;;) {
                const ssize_t n = ::recv(udp, buf, sizeof buf, 0);
                if (n < 0) break;
                reasm.add(buf, static_cast<std::size_t>(n));
            }
        }
        if (!opt.quiet && Clock::now() - last_report >= std::chrono::seconds(1)) {
            last_report = Clock::now();
            std::cout << "STATS fragments=" << reasm.fragments() << " frames=" << reasm.frames_completed()
                      << " incomplete=" << reasm.frames_abandoned() << " heartbeats=" << heartbeats << std::endl;
        }
    }

    if (!server_closed) {  // graceful goodbye
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
                if (!opt.quiet && !starts_with(*line, "HB ")) std::cout << "<< " << *line << std::endl;
        }
    }

    std::cout << "SUMMARY fragments=" << reasm.fragments() << " frames=" << reasm.frames_completed()
              << " incomplete=" << reasm.frames_abandoned() << " invalid=" << reasm.invalid()
              << " heartbeats=" << heartbeats << std::endl;
    ::close(tcp);
    ::close(udp);
    return exit_code;
}
