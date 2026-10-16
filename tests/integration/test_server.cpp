// End-to-end tests: a real Server on an ephemeral port, real sockets, and for the
// kill tests a real radar_client child process.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "server.hpp"
#include "test_client.hpp"

using namespace radar;
using namespace radar::test;

namespace {

std::size_t count_state(const Server& s, SlotState state) {
    std::size_t n = 0;
    for (const auto& snap : s.snapshot()) n += snap.state == state;
    return n;
}

// fork + exec radar_client. argv is built before fork(): after fork() in a
// multi-threaded process the child may only call async-signal-safe functions.
pid_t spawn_client(std::uint16_t port) {
    std::vector<std::string> args = {RADAR_CLIENT_PATH, "--port", std::to_string(port), "--quiet"};
    std::vector<char*> argv;
    for (auto& a : args) argv.push_back(a.data());
    argv.push_back(nullptr);

    const pid_t pid = ::fork();
    if (pid == 0) {
        const int devnull = ::open("/dev/null", O_WRONLY);
        ::dup2(devnull, STDOUT_FILENO);
        ::dup2(devnull, STDERR_FILENO);
        ::execv(argv[0], argv.data());
        ::_exit(127);
    }
    return pid;
}

void kill_and_reap(pid_t pid, int sig) {
    ::kill(pid, sig);
    if (sig != SIGSTOP) {
        int status = 0;
        ::waitpid(pid, &status, 0);
    }
}

class ServerIT : public ::testing::Test {
protected:
    ServerIT() {
        cfg.tcp_port = 0;  // ephemeral: tests can run in parallel
        cfg.log_level = LogLevel::Warn;
        cfg.heartbeat_interval = 200ms;
        cfg.zombie_timeout = 1000ms;
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

    // Connects and expects the welcome banner.
    void connect_ok(TestClient& c) {
        ASSERT_TRUE(c.connect(server->port()));
        auto l = c.read_reply();
        ASSERT_TRUE(l);
        EXPECT_EQ(l->rfind("WELCOME", 0), 0u) << *l;
    }

    void start_streaming(TestClient& c) {
        const auto udp_port = c.open_udp();
        ASSERT_TRUE(c.send_line("REGISTER " + std::to_string(udp_port)));
        auto r = c.read_reply();
        ASSERT_TRUE(r);
        ASSERT_EQ(r->rfind("OK REGISTERED", 0), 0u) << *r;
        ASSERT_TRUE(c.send_line("START"));
        r = c.read_reply();
        ASSERT_TRUE(r);
        ASSERT_EQ(*r, "OK STREAMING");
    }

    Config cfg;
    std::unique_ptr<Server> server;
};

}  // namespace

// ---- connection limit -------------------------------------------------------

TEST_F(ServerIT, FourthConcurrentClientGetsErrFullAndIsClosed) {
    ASSERT_TRUE(start());
    std::array<TestClient, 3> ok;
    for (auto& c : ok) ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    EXPECT_EQ(server->active_clients(), 3u);

    TestClient fourth;
    ASSERT_TRUE(fourth.connect(server->port()));
    auto l = fourth.read_reply();
    ASSERT_TRUE(l);
    EXPECT_EQ(*l, "ERR FULL");
    EXPECT_TRUE(fourth.wait_eof(1000ms));
    EXPECT_EQ(server->active_clients(), 3u);
    EXPECT_EQ(server->stats().rejected_full, 1u);

    // The three existing sessions are unaffected.
    for (auto& c : ok) {
        ASSERT_TRUE(c.send_line("PING x"));
        EXPECT_EQ(c.read_reply(), "PONG x");
    }

    // A freed slot is immediately reusable.
    ASSERT_TRUE(ok[1].send_line("QUIT"));
    EXPECT_EQ(ok[1].read_reply(), "BYE");
    EXPECT_TRUE(ok[1].wait_eof(1000ms));
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 2; }, 1000ms));
    TestClient again;
    ASSERT_NO_FATAL_FAILURE(connect_ok(again));
}

TEST_F(ServerIT, FourSimultaneousConnectsExactlyOneRejected) {
    ASSERT_TRUE(start());
    std::array<TestClient, 4> c;
    std::array<std::string, 4> first;
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i)
        threads.emplace_back([&, i] {
            if (c[i].connect(server->port())) first[i] = c[i].read_reply().value_or("<none>");
        });
    for (auto& t : threads) t.join();

    int welcomed = 0, rejected = 0;
    for (const auto& l : first) {
        welcomed += l.rfind("WELCOME", 0) == 0;
        rejected += l == "ERR FULL";
    }
    EXPECT_EQ(welcomed, 3);
    EXPECT_EQ(rejected, 1);
}

// ---- streaming --------------------------------------------------------------

TEST_F(ServerIT, ThreeClientsStreamConcurrently) {
    ASSERT_TRUE(start());
    const std::size_t fpf = server->fragments_per_frame();
    ASSERT_EQ(fpf, 4u);  // default 64 tracks / 512-byte datagrams

    std::array<TestClient, 3> c;
    for (auto& cl : c) {
        ASSERT_NO_FATAL_FAILURE(connect_ok(cl));
        ASSERT_NO_FATAL_FAILURE(start_streaming(cl));
    }

    constexpr auto kStream = 1500ms;
    std::vector<std::thread> threads;
    for (auto& cl : c) threads.emplace_back([&cl] { cl.pump(kStream); });
    for (auto& t : threads) t.join();

    for (std::size_t i = 0; i < c.size(); ++i) {
        ASSERT_TRUE(c[i].send_line("STOP"));
        ASSERT_EQ(c[i].read_reply(), "OK STOPPED");
    }
    for (auto& cl : c) cl.pump(150ms);  // drain datagrams still in flight

    // ~1500/35 = 43 frames per client (the first client started a bit earlier).
    for (std::size_t i = 0; i < c.size(); ++i) {
        ASSERT_TRUE(c[i].send_line("STATUS"));
        auto status = c[i].read_reply();
        ASSERT_TRUE(status);
        const auto sent = status_field(*status, "fragments_sent");
        const auto& rx = c[i].udp_stats();

        std::cout << "  client " << i << ": received fragments=" << rx.fragments()
                  << " frames=" << rx.frames_completed() << " | server sent fragments=" << sent << "\n";

        EXPECT_GE(rx.frames_completed(), 30u);
        EXPECT_LE(rx.frames_completed(), 60u);
        EXPECT_EQ(rx.invalid(), 0u);
        EXPECT_LE(rx.fragments(), sent);
        EXPECT_GE(rx.fragments(), sent * 95 / 100);  // loopback: essentially lossless
        EXPECT_GE(c[i].heartbeats(), 5u);
    }
    EXPECT_EQ(server->active_clients(), 3u);
    EXPECT_EQ(server->stats().frames_dropped, 0u);
}

TEST_F(ServerIT, StopHaltsTheStream) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    c.pump(300ms);
    ASSERT_TRUE(c.send_line("STOP"));
    ASSERT_EQ(c.read_reply(), "OK STOPPED");
    c.pump(100ms);
    const auto before = c.udp_stats().fragments();
    EXPECT_GT(before, 0u);
    c.pump(300ms);
    EXPECT_EQ(c.udp_stats().fragments(), before);
}

// ---- kill -9: slot recovery -------------------------------------------------

TEST_F(ServerIT, KillDash9ClientReleasesSlotImmediately) {
    cfg.zombie_timeout = 30s;  // prove recovery comes from EOF, not from the zombie timer
    cfg.heartbeat_interval = 10s;
    ASSERT_TRUE(start());

    const pid_t pid = spawn_client(server->port());
    ASSERT_GT(pid, 0);
    ASSERT_TRUE(wait_until([&] { return count_state(*server, SlotState::Streaming) == 1; }, 3000ms));

    const auto t0 = SteadyClock::now();
    kill_and_reap(pid, SIGKILL);
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 2000ms));
    const auto ms = std::chrono::duration_cast<Ms>(SteadyClock::now() - t0).count();
    std::cout << "  slot released " << ms << " ms after SIGKILL\n";
    EXPECT_LT(ms, 1000);
    const auto st = server->stats();
    EXPECT_EQ(st.closed_by(CloseReason::PeerClosed) + st.closed_by(CloseReason::Reset), 1u);
    EXPECT_EQ(st.closed_by(CloseReason::Timeout), 0u);
}

TEST_F(ServerIT, KillDash9WhileFullLetsANewClientIn) {
    ASSERT_TRUE(start());
    std::array<pid_t, 3> pids{};
    for (auto& p : pids) p = spawn_client(server->port());
    ASSERT_TRUE(wait_until([&] { return count_state(*server, SlotState::Streaming) == 3; }, 3000ms));

    TestClient rejected;
    ASSERT_TRUE(rejected.connect(server->port()));
    EXPECT_EQ(rejected.read_reply(), "ERR FULL");

    kill_and_reap(pids[1], SIGKILL);
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 2; }, 2000ms));

    TestClient newcomer;
    ASSERT_NO_FATAL_FAILURE(connect_ok(newcomer));
    ASSERT_NO_FATAL_FAILURE(start_streaming(newcomer));
    newcomer.pump(300ms);
    EXPECT_GT(newcomer.udp_stats().frames_completed(), 0u);

    kill_and_reap(pids[0], SIGKILL);
    kill_and_reap(pids[2], SIGKILL);
}

// ---- zombies ----------------------------------------------------------------

// A frozen process (SIGSTOP) keeps its TCP connection open, so no FIN ever arrives.
// Only the heartbeat timeout can detect it.
TEST_F(ServerIT, FrozenClientIsReapedByHeartbeatTimeout) {
    cfg.heartbeat_interval = 100ms;
    cfg.zombie_timeout = 400ms;
    ASSERT_TRUE(start());

    const pid_t pid = spawn_client(server->port());
    ASSERT_TRUE(wait_until([&] { return count_state(*server, SlotState::Streaming) == 1; }, 3000ms));

    // Alive and answering: must survive several timeout periods.
    std::this_thread::sleep_for(1000ms);
    ASSERT_EQ(server->active_clients(), 1u);

    const auto t0 = SteadyClock::now();
    ::kill(pid, SIGSTOP);
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 3000ms));
    const auto ms = std::chrono::duration_cast<Ms>(SteadyClock::now() - t0).count();
    std::cout << "  frozen client reaped " << ms << " ms after SIGSTOP\n";
    // Detected between zombie_timeout - one heartbeat (last PONG may predate SIGSTOP)
    // and zombie_timeout + scheduling slack.
    EXPECT_GE(ms, 250);
    EXPECT_LE(ms, 900);
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 1u);

    ::kill(pid, SIGKILL);
    ::waitpid(pid, nullptr, 0);
}

TEST_F(ServerIT, SilentClientIsZombieResponsiveClientIsNot) {
    cfg.heartbeat_interval = 100ms;
    cfg.zombie_timeout = 400ms;
    ASSERT_TRUE(start());

    TestClient silent, healthy;
    silent.auto_pong = false;
    ASSERT_NO_FATAL_FAILURE(connect_ok(silent));
    ASSERT_NO_FATAL_FAILURE(connect_ok(healthy));

    healthy.pump(1000ms);  // answers every HB meanwhile; `silent` never reads or writes

    EXPECT_TRUE(silent.wait_eof(500ms));
    EXPECT_FALSE(healthy.eof());
    EXPECT_EQ(server->active_clients(), 1u);
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 1u);
    EXPECT_GE(healthy.heartbeats(), 8u);
}

TEST_F(ServerIT, ZombieGetsByeTimeoutBeforeClose) {
    cfg.heartbeat_interval = 100ms;
    cfg.zombie_timeout = 300ms;
    ASSERT_TRUE(start());
    TestClient c;
    c.auto_pong = false;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    EXPECT_EQ(c.read_reply(1500ms), "BYE TIMEOUT");  // read_reply skips the unanswered HBs
    EXPECT_TRUE(c.wait_eof(500ms));
}

// ---- graceful close / shutdown ----------------------------------------------

TEST_F(ServerIT, QuitClosesGracefully) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    ASSERT_TRUE(c.send_line("QUIT"));
    EXPECT_EQ(c.read_reply(), "BYE");
    EXPECT_TRUE(c.wait_eof(1000ms));
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 1000ms));
    EXPECT_EQ(server->stats().closed_by(CloseReason::Quit), 1u);
}

TEST_F(ServerIT, ServerShutdownSaysByeToEveryone) {
    ASSERT_TRUE(start());
    std::array<TestClient, 2> c;
    for (auto& cl : c) {
        ASSERT_NO_FATAL_FAILURE(connect_ok(cl));
        ASSERT_NO_FATAL_FAILURE(start_streaming(cl));
    }
    server->stop();
    for (auto& cl : c) {
        EXPECT_EQ(cl.read_reply(), "BYE SHUTDOWN");
        EXPECT_TRUE(cl.wait_eof(1000ms));
    }
    EXPECT_EQ(server->stats().closed_by(CloseReason::Shutdown), 2u);
}

// ---- protocol errors --------------------------------------------------------

TEST_F(ServerIT, ProtocolErrorsAreReportedAndConnectionSurvives) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));

    c.send_line("START");
    EXPECT_EQ(c.read_reply(), "ERR NOT_REGISTERED");
    c.send_line("FIRE");
    EXPECT_EQ(c.read_reply(), "ERR UNKNOWN_COMMAND FIRE");
    c.send_line("REGISTER 99999");
    auto r = c.read_reply();
    ASSERT_TRUE(r);
    EXPECT_EQ(r->rfind("ERR BAD_ARGUMENT", 0), 0u) << *r;
    c.send_line("ping 7");
    EXPECT_EQ(c.read_reply(), "PONG 7");
    c.send_line("STATUS");
    r = c.read_reply();
    ASSERT_TRUE(r);
    EXPECT_NE(r->find("state=CONNECTED"), std::string::npos) << *r;
}

TEST_F(ServerIT, OverlongLineClosesConnection) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    c.send_line(std::string(1000, 'A'));
    EXPECT_EQ(c.read_reply(), "ERR LINE_TOO_LONG");
    EXPECT_TRUE(c.wait_eof(1000ms));
    EXPECT_EQ(server->stats().closed_by(CloseReason::Protocol), 1u);
}

// ---- back-pressure ----------------------------------------------------------

// Producer at 1 ms with a 1-frame queue and ~100 datagrams per frame. Whether the
// sender keeps up depends on the machine (and on sanitizers), so this does not require
// drops; it checks the accounting invariant: nothing vanishes uncounted. Deterministic
// drop/count behaviour is covered by the BoundedQueue unit tests.
TEST_F(ServerIT, QueueAccountsForEveryFrame) {
    cfg.frame_interval = 1ms;
    cfg.queue_capacity = 1;
    cfg.tracks_per_frame = 2000;  // ~48 KB per frame -> ~100 datagrams of 512 bytes
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    c.pump(500ms);

    const auto snap = server->snapshot()[0];
    const auto st = server->stats();
    std::cout << "  produced=" << st.frames_produced << " sent=" << snap.frames_sent
              << " dropped=" << snap.frames_dropped << "\n";
    EXPECT_GT(snap.frames_sent, 0u);
    EXPECT_EQ(st.frames_dropped, snap.frames_dropped);
    // Every frame produced while streaming was either sent, dropped, or is still queued.
    EXPECT_GE(st.frames_produced, snap.frames_sent + snap.frames_dropped);
}
