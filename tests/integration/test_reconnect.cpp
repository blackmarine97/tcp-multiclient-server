// radar_client reconnecting against a real Server: the client is a real child
// process, and what it did is observed from the server's side.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "child_process.hpp"
#include "server.hpp"
#include "socket_util.hpp"
#include "test_client.hpp"

using namespace radar;
using namespace radar::test;

namespace {

const std::vector<std::string> kFastReconnect = {"--reconnect-min-ms", "50", "--reconnect-max-ms", "200"};

std::vector<std::string> fast_reconnect_plus(std::vector<std::string> more) {
    auto all = kFastReconnect;
    all.insert(all.end(), more.begin(), more.end());
    return all;
}

class ReconnectIT : public ::testing::Test {
protected:
    ReconnectIT() {
        cfg.tcp_port = 0;
        cfg.log_level = LogLevel::Off;
        cfg.heartbeat_interval = 100ms;
        cfg.zombie_timeout = 400ms;
    }

    void TearDown() override {
        if (server) server->stop();
    }

    bool start() {
        server = std::make_unique<Server>(cfg);
        std::string err;
        const bool ok = server->start(&err);
        EXPECT_TRUE(ok) << err;
        return ok;
    }

    std::size_t streaming() const {
        std::size_t n = 0;
        for (const auto& snap : server->snapshot()) n += snap.state == SlotState::Streaming;
        return n;
    }

    Config cfg;
    std::unique_ptr<Server> server;
};

// A port nothing listens on: bind an ephemeral one, then release it.
std::uint16_t unused_port() {
    UniqueFd s(::socket(AF_INET, SOCK_STREAM, 0));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(s.get(), reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t len = sizeof a;
    ::getsockname(s.get(), reinterpret_cast<sockaddr*>(&a), &len);
    return ntohs(a.sin_port);
}

}  // namespace

// Client frozen long enough to be reaped as a zombie, then resumed: it finds the
// connection gone, connects again and streams again without any help.
TEST_F(ReconnectIT, ClientReapedAsZombieComesBackAfterResume) {
    ASSERT_TRUE(start());
    Child client(server->port(), kFastReconnect);
    ASSERT_TRUE(wait_until([&] { return streaming() == 1; }, 3000ms));

    client.signal(SIGSTOP);
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 3000ms));
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 1u);

    client.signal(SIGCONT);
    ASSERT_TRUE(wait_until([&] { return streaming() == 1; }, 3000ms));
    EXPECT_EQ(server->stats().accepted, 2u);

    // The new session really receives data: frames are being sent again.
    ASSERT_TRUE(wait_until([&] { return server->snapshot()[0].frames_sent > 3; }, 3000ms));
}

TEST_F(ReconnectIT, FullServerIsRetriedUntilASlotFrees) {
    cfg.heartbeat_interval = 10s;  // the three holders below only need to stay connected
    cfg.zombie_timeout = 30s;
    ASSERT_TRUE(start());

    std::array<TestClient, 3> holders;
    for (auto& h : holders) {
        ASSERT_TRUE(h.connect(server->port()));
        ASSERT_TRUE(h.read_reply());
    }

    Child client(server->port(), kFastReconnect);
    ASSERT_TRUE(wait_until([&] { return server->stats().rejected_full >= 2; }, 3000ms))
        << "the client should keep retrying instead of exiting";
    EXPECT_EQ(streaming(), 0u);

    ASSERT_TRUE(holders[0].send_line("QUIT"));
    ASSERT_TRUE(wait_until([&] { return streaming() == 1; }, 3000ms));
    EXPECT_EQ(server->active_clients(), 3u);
}

TEST_F(ReconnectIT, SurvivesServerRestartOnTheSamePort) {
    ASSERT_TRUE(start());
    const std::uint16_t port = server->port();
    Child client(port, kFastReconnect);
    ASSERT_TRUE(wait_until([&] { return streaming() == 1; }, 3000ms));

    server->stop();
    server.reset();
    std::this_thread::sleep_for(300ms);  // client sees refused connections meanwhile

    cfg.tcp_port = port;
    ASSERT_TRUE(start());
    EXPECT_TRUE(wait_until([&] { return streaming() == 1; }, 3000ms));
}

// A server that goes quiet without closing anything (frozen, or the network died):
// TCP reports nothing, so the client has to notice the missing heartbeats itself.
TEST_F(ReconnectIT, SilentServerIsDetectedByTheClient) {
    UniqueFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(listener.get(), reinterpret_cast<sockaddr*>(&a), sizeof a), 0);
    ASSERT_EQ(::listen(listener.get(), 4), 0);
    socklen_t len = sizeof a;
    ::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&a), &len);

    auto accept_within = [&](int timeout_ms) {
        pollfd p{listener.get(), POLLIN, 0};
        return ::poll(&p, 1, timeout_ms) > 0 ? UniqueFd(::accept(listener.get(), nullptr, nullptr)) : UniqueFd();
    };

    Child client(ntohs(a.sin_port), kFastReconnect);
    UniqueFd first = accept_within(3000);
    ASSERT_TRUE(first.valid());
    static constexpr char kWelcome[] = "WELCOME RADARSIM/1.0 slot=0 max=3 hb_ms=100 timeout_ms=300\n";
    ASSERT_GT(::send(first.get(), kWelcome, sizeof kWelcome - 1, MSG_NOSIGNAL), 0);

    // `first` stays open and says nothing more. The client must give up on it and come back.
    const auto t0 = SteadyClock::now();
    UniqueFd second = accept_within(3000);
    ASSERT_TRUE(second.valid());
    EXPECT_GE(SteadyClock::now() - t0, 250ms);  // not before the announced timeout_ms
}

TEST_F(ReconnectIT, GivesUpAfterMaxRetriesWithExitCode3) {
    Child client(unused_port(), fast_reconnect_plus({"--max-retries", "3"}));
    EXPECT_EQ(client.wait_exit(5000ms), 3);
}

TEST_F(ReconnectIT, CtrlCInterruptsALongBackoffPromptly) {
    Child client(unused_port(), {"--reconnect-min-ms", "60000", "--reconnect-max-ms", "60000"});
    std::this_thread::sleep_for(200ms);  // now sleeping in its first 60 s backoff
    client.signal(SIGINT);
    EXPECT_EQ(client.wait_exit(1000ms), 0);
}

TEST_F(ReconnectIT, DurationEndsTheRunEvenWhileRetrying) {
    Child client(unused_port(), {"--reconnect-min-ms", "60000", "--reconnect-max-ms", "60000",
                                 "--duration-ms", "300"});
    EXPECT_EQ(client.wait_exit(2000ms), 0);
}

// A full server is retried like any other failure, and --max-retries bounds it.
TEST_F(ReconnectIT, FullServerGivesUpAfterMaxRetriesWithExitCode3) {
    cfg.max_clients = 1;
    cfg.heartbeat_interval = 10s;
    cfg.zombie_timeout = 30s;
    ASSERT_TRUE(start());
    TestClient holder;
    ASSERT_TRUE(holder.connect(server->port()));
    ASSERT_TRUE(holder.read_reply());

    Child client(server->port(), fast_reconnect_plus({"--max-retries", "2"}));
    EXPECT_EQ(client.wait_exit(3000ms), 3);
    EXPECT_EQ(server->stats().rejected_full, 3u);  // first attempt + 2 retries
}

// Reconnecting is the default: no option is needed, even with the default delays.
TEST_F(ReconnectIT, ReconnectsWithNoOptions) {
    ASSERT_TRUE(start());
    const std::uint16_t port = server->port();
    Child client(port, {});
    ASSERT_TRUE(wait_until([&] { return streaming() == 1; }, 3000ms));

    server->stop();  // BYE SHUTDOWN must not end the client
    server.reset();
    cfg.tcp_port = port;
    ASSERT_TRUE(start());
    EXPECT_TRUE(wait_until([&] { return streaming() == 1; }, 5000ms));
}
