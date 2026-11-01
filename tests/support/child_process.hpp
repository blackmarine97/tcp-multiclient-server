#pragma once

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "test_client.hpp"

namespace radar::test {

// A radar_client child process that is always cleaned up, even when a test fails.
class Child {
public:
    Child(std::uint16_t port, std::vector<std::string> extra) {
        std::vector<std::string> args = {RADAR_CLIENT_PATH, "--port", std::to_string(port), "--quiet"};
        args.insert(args.end(), extra.begin(), extra.end());
        std::vector<char*> argv;
        for (auto& a : args) argv.push_back(a.data());
        argv.push_back(nullptr);

        pid_ = ::fork();  // argv is built first: only async-signal-safe calls after fork()
        if (pid_ == 0) {
            const int devnull = ::open("/dev/null", O_WRONLY);
            ::dup2(devnull, STDOUT_FILENO);
            ::dup2(devnull, STDERR_FILENO);
            ::execv(argv[0], argv.data());
            ::_exit(127);
        }
    }

    ~Child() {
        if (pid_ > 0) {
            ::kill(pid_, SIGKILL);
            ::waitpid(pid_, nullptr, 0);
        }
    }

    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;

    void signal(int sig) { ::kill(pid_, sig); }

    // Exit code once the process has exited by itself; nullopt on timeout or if it was killed.
    std::optional<int> wait_exit(Ms timeout) {
        int status = 0;
        const bool done = wait_until([&] { return ::waitpid(pid_, &status, WNOHANG) == pid_; }, timeout);
        if (!done) return std::nullopt;
        pid_ = -1;
        return WIFEXITED(status) ? std::optional<int>(WEXITSTATUS(status)) : std::nullopt;
    }

private:
    pid_t pid_ = -1;
};

}  // namespace radar::test
