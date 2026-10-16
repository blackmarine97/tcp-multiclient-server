// radar_server - multi-client fake track server.
//
//   radar_server [-c config_file] [-p tcp_port]
//
// Stops gracefully on SIGINT / SIGTERM.

#include <pthread.h>
#include <signal.h>

#include <cstdlib>
#include <iostream>
#include <string>

#include "config.hpp"
#include "server.hpp"

namespace {

void usage(const char* argv0) {
    std::cerr << "usage: " << argv0 << " [-c config_file] [-p tcp_port]\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string config_path;
    int port_override = -1;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if ((a == "-c" || a == "--config") && i + 1 < argc) {
            config_path = argv[++i];
        } else if ((a == "-p" || a == "--port") && i + 1 < argc) {
            port_override = std::atoi(argv[++i]);
        } else {
            usage(argv[0]);
            return a == "-h" || a == "--help" ? 0 : 1;
        }
    }

    radar::Config cfg;
    if (!config_path.empty()) {
        std::string err;
        auto loaded = radar::load_config_file(config_path, &err);
        if (!loaded) {
            std::cerr << "config error: " << err << "\n";
            return 1;
        }
        cfg = *loaded;
    }
    if (port_override >= 0) {
        if (port_override > 65535) {
            std::cerr << "invalid port\n";
            return 1;
        }
        cfg.tcp_port = static_cast<std::uint16_t>(port_override);
    }

    // Block the stop signals in every thread (threads inherit the mask), then
    // receive them synchronously here with sigwait(): no async-signal-safety traps.
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);
    signal(SIGPIPE, SIG_IGN);

    radar::Server server(cfg);
    std::string err;
    if (!server.start(&err)) {
        std::cerr << "failed to start: " << err << "\n";
        return 1;
    }
    std::cout << "radar_server listening on port " << server.port() << std::endl;

    int sig = 0;
    sigwait(&stop_signals, &sig);
    radar::log_msg(radar::LogLevel::Info, "received ", sig == SIGINT ? "SIGINT" : "SIGTERM",
                   ", shutting down");
    server.stop();
    return 0;
}
