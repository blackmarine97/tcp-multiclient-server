// End-to-end tests: a real Server on an ephemeral port, real sockets, and for the
// kill tests a real radar_client child process.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
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

namespace {

Config slow_consumer_config(Config cfg) {
    cfg.frame_interval = 1ms;
    cfg.queue_capacity = 1;
    cfg.tracks_per_frame = 1000;                       // 24 KB per frame
    cfg.max_datagram_bytes = kFragmentHeaderSize + 1;  // -> 24016 datagrams per frame
    return cfg;
}

// With the sender of slot 0 held, its queue must overflow: the producer keeps its pace,
// every discarded frame shows up in the counters, and sending resumes on release.
void expect_real_drops(Server& server, TestClient& c, const Config& cfg) {
    {
        auto held = server.hold_sender_for_test(0);
        const auto produced_before = server.stats().frames_produced;
        const auto sent_before = server.snapshot()[0].frames_sent;
        c.pump(400ms);

        const auto produced = server.stats().frames_produced - produced_before;
        const auto snap = server.snapshot()[0];
        std::cout << "  while stuck: produced=" << produced << " sent=" << snap.frames_sent - sent_before
                  << " dropped=" << snap.frames_dropped << "\n";

        EXPECT_GE(produced, static_cast<std::uint64_t>(200ms / cfg.frame_interval));  // half pace is plenty
        EXPECT_EQ(snap.frames_sent, sent_before);
        EXPECT_EQ(snap.queue_depth, cfg.queue_capacity);
        EXPECT_GE(snap.frames_dropped + cfg.queue_capacity + 1, produced);  // only the queue and one in hand survive
        EXPECT_GT(snap.frames_dropped, 0u);
        EXPECT_EQ(server.stats().frames_dropped, snap.frames_dropped);

        ASSERT_TRUE(c.send_line("STATUS"));
        const auto status = c.read_reply();
        ASSERT_TRUE(status);
        EXPECT_GT(status_field(*status, "dropped"), 0u);
    }

    const auto sent_at_release = server.snapshot()[0].frames_sent;
    EXPECT_TRUE(wait_until([&] { return server.snapshot()[0].frames_sent > sent_at_release + 2; }, 2000ms));

    // The client sees what the server dropped, as gaps in the frame numbers.
    c.pump(100ms);
    const auto dropped = server.snapshot()[0].frames_dropped;
    std::cout << "  client saw lost=" << c.stream_stats().lost() << " (server dropped " << dropped << ")\n";
    EXPECT_GT(c.stream_stats().lost(), 0u);
    EXPECT_LE(c.stream_stats().lost(), dropped);
    EXPECT_GE(c.stream_stats().lost() + cfg.queue_capacity + 1, dropped);
}

TEST_F(ServerIT, FramePeriodIsCloseToConfigured) {
    ASSERT_TRUE(start());  // default 35 ms
    std::this_thread::sleep_for(1500ms);
    const auto period = server->stats().frame_period;
    std::cout << "  " << period.count() << " intervals: avg=" << period.mean_us() / 1000.0
              << " ms min=" << period.min_us() / 1000.0 << " ms max=" << period.max_us() / 1000.0
              << " ms jitter=" << period.jitter_us() / 1000.0 << " ms\n";
    EXPECT_GE(period.count(), 35u);
    // The schedule is fixed-rate, so a late wake-up does not push later frames back.
    // Single intervals depend on the OS scheduler and are reported, not asserted.
    EXPECT_NEAR(period.mean_us(), 35000.0, 1500.0);
}

TEST_F(ServerIT, HealthyLoopbackClientSeesNoLossAndLowLatency) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    c.pump(1000ms);
    const auto& s = c.stream_stats();
    std::cout << "  received=" << s.received() << " lost=" << s.lost() << " latency avg="
              << s.latency_mean_us() / 1000.0 << " ms max=" << s.latency_max_us() / 1000.0 << " ms\n";
    EXPECT_GE(s.received(), 20u);
    EXPECT_EQ(s.lost(), 0u);
    EXPECT_GE(s.latency_min_us(), 0);
    EXPECT_LT(s.latency_mean_us(), 10000.0);
}

}  // namespace

TEST_F(ServerIT, StuckConsumerDropsOldestFramesAndCountsThem) {
    cfg.frame_interval = 5ms;
    cfg.queue_capacity = 2;
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    ASSERT_NO_FATAL_FAILURE(expect_real_drops(*server, c, cfg));
}

TEST_F(ServerIT, StuckConsumerDropsNewestFramesAndCountsThem) {
    cfg.frame_interval = 5ms;
    cfg.queue_capacity = 2;
    cfg.queue_overflow = OverflowPolicy::DropNewest;
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    ASSERT_NO_FATAL_FAILURE(expect_real_drops(*server, c, cfg));
}

// The point of per-client queues: one stuck client costs the others nothing.
TEST_F(ServerIT, StuckConsumerDoesNotAffectOtherClients) {
    cfg.frame_interval = 5ms;
    cfg.queue_capacity = 2;
    ASSERT_TRUE(start());
    TestClient stuck, other;
    ASSERT_NO_FATAL_FAILURE(connect_ok(stuck));   // slot 0
    ASSERT_NO_FATAL_FAILURE(connect_ok(other));   // slot 1
    ASSERT_NO_FATAL_FAILURE(start_streaming(stuck));
    ASSERT_NO_FATAL_FAILURE(start_streaming(other));

    auto held = server->hold_sender_for_test(0);
    const auto produced_before = server->stats().frames_produced;
    const auto other_before = other.udp_stats().frames_completed();
    other.pump(400ms);
    const auto produced = server->stats().frames_produced - produced_before;
    const auto received = other.udp_stats().frames_completed() - other_before;
    std::cout << "  slot 0 stuck: produced=" << produced << " other client received=" << received << "\n";

    EXPECT_GT(server->snapshot()[0].frames_dropped, 0u);
    EXPECT_EQ(server->snapshot()[1].frames_dropped, 0u);
    EXPECT_GE(received + 2, produced);  // the other client got essentially every frame
    EXPECT_GE(produced, 40u);           // 80 expected at 5 ms
}

// STOP has to stay final even in the middle of a long frame.
TEST_F(ServerIT, StopTakesEffectInTheMiddleOfALongFrame) {
    cfg = slow_consumer_config(cfg);
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    c.pump(100ms);

    ASSERT_TRUE(c.send_line("STOP"));
    ASSERT_EQ(c.read_reply(1000ms), "OK STOPPED");
    const auto at_stop = server->snapshot()[0].fragments_sent;
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(server->snapshot()[0].fragments_sent, at_stop);
}

// ---- stalled client ---------------------------------------------------------

// A client that keeps sending but stops reading never looks dead to the heartbeat
// check (bytes keep arriving), so only the output backlog limit can catch it.
TEST_F(ServerIT, ClientThatStopsReadingIsReapedAsStalled) {
    cfg.max_commands_per_sec = 0;  // about output backlog, not about the rate limit
    cfg.max_line_bytes = 60000;    // lets PING produce large replies
    cfg.heartbeat_interval = 10s;  // no help from the timer...
    cfg.zombie_timeout = 30s;      // ...this has to be the backlog check
    ASSERT_TRUE(start());

    TestClient healthy;
    ASSERT_NO_FATAL_FAILURE(connect_ok(healthy));

    UniqueFd stalled(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    const int small = 4096;
    ::setsockopt(stalled.get(), SOL_SOCKET, SO_RCVBUF, &small, sizeof small);  // before connect()
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(server->port());
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::connect(stalled.get(), reinterpret_cast<sockaddr*>(&a), sizeof a), 0);
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 2; }, 1000ms));

    const std::string ping = "PING " + std::string(50000, 'x') + "\n";
    const auto deadline = SteadyClock::now() + 15s;
    while (server->stats().closed_by(CloseReason::Stalled) == 0 && SteadyClock::now() < deadline) {
        const ssize_t n = ::send(stalled.get(), ping.data(), ping.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) break;  // server hung up
        if (n < 0) std::this_thread::sleep_for(1ms);
    }

    ASSERT_TRUE(wait_until([&] { return server->stats().closed_by(CloseReason::Stalled) == 1; }, 2000ms));
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 0u);
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 1; }, 1000ms));

    // The stalled client's slot is free and the other client never noticed.
    ASSERT_TRUE(healthy.send_line("PING ok"));
    EXPECT_EQ(healthy.read_reply(), "PONG ok");
    TestClient next;
    ASSERT_NO_FATAL_FAILURE(connect_ok(next));
}
