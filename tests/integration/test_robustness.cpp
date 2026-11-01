// Stress and edge cases beyond the assignment's required tests: claims the design
// makes that a normal run never exercises.

#include <gtest/gtest.h>

#include <dirent.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>

#include <array>
#include <atomic>
#include <iostream>
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

int open_fd_count() {
    int n = 0;
    DIR* d = ::opendir("/proc/self/fd");
    if (!d) return -1;
    while (::readdir(d)) ++n;
    ::closedir(d);
    return n;
}

class RobustnessIT : public ::testing::Test {
protected:
    RobustnessIT() {
        cfg.tcp_port = 0;
        cfg.log_level = LogLevel::Off;
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

    void connect_ok(TestClient& c) {
        ASSERT_TRUE(c.connect(server->port()));
        const auto l = c.read_reply();
        ASSERT_TRUE(l);
        ASSERT_EQ(l->rfind("WELCOME", 0), 0u) << *l;
    }

    void start_streaming(TestClient& c) {
        ASSERT_TRUE(c.send_line("REGISTER " + std::to_string(c.open_udp())));
        const auto r = c.read_reply();
        ASSERT_TRUE(r);
        ASSERT_EQ(r->rfind("OK REGISTERED", 0), 0u) << *r;
        ASSERT_TRUE(c.send_line("START"));
        ASSERT_EQ(c.read_reply(), "OK STREAMING");
    }

    Config cfg;
    std::unique_ptr<Server> server;
};

// Frames slow enough to send that frames stay queued for a long time.
void make_frames_slow_to_send(Config& cfg) {
    cfg.frame_interval = 1ms;
    cfg.tracks_per_frame = 1000;                       // 24 KB per frame
    cfg.max_datagram_bytes = kFragmentHeaderSize + 1;  // -> 24016 datagrams per frame
}

}  // namespace

// ---- connection churn -------------------------------------------------------

// Many short sessions from several threads, ending in every way a session can end.
// Afterwards nothing may be left behind: no file descriptor, no slot, no uncounted close.
TEST_F(RobustnessIT, ConnectionChurnLeavesNothingBehind) {
    cfg.frame_interval = 5ms;
    cfg.heartbeat_interval = 10s;
    cfg.zombie_timeout = 30s;
    ASSERT_TRUE(start());
    std::this_thread::sleep_for(20ms);
    const int fds_before = open_fd_count();

    constexpr int kMaxSessions = 4000;  // stays well below the ephemeral port range
    std::atomic<int> sessions{0}, welcomed{0};
    const auto deadline = SteadyClock::now() + 3s;

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t)
        threads.emplace_back([&] {
            while (SteadyClock::now() < deadline) {
                const int n = sessions.fetch_add(1);
                if (n >= kMaxSessions) return;
                TestClient c;
                if (!c.connect(server->port())) continue;
                const auto first = c.read_reply();
                if (!first || first->rfind("WELCOME", 0) != 0) continue;
                ++welcomed;
                switch (n % 4) {
                    case 0:  // polite
                        c.send_line("QUIT");
                        c.read_reply();
                        break;
                    case 1:  // FIN with commands still unanswered
                        c.send_line("STATUS");
                        break;
                    case 2:  // streaming when the connection vanishes
                        c.send_line("REGISTER " + std::to_string(c.open_udp()));
                        c.send_line("START");
                        c.read_reply();
                        c.read_reply();
                        break;
                    case 3: {  // RST
                        const linger hard{1, 0};
                        ::setsockopt(c.tcp_fd(), SOL_SOCKET, SO_LINGER, &hard, sizeof hard);
                        break;
                    }
                }
            }
        });
    for (auto& t : threads) t.join();

    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 3000ms));
    std::this_thread::sleep_for(50ms);

    const auto st = server->stats();
    std::uint64_t closed = 0;
    for (const auto c : st.closed) closed += c;
    std::cout << "  sessions=" << std::min<int>(sessions, kMaxSessions) << " welcomed=" << welcomed
              << " rejected_full=" << st.rejected_full << " fds " << fds_before << " -> " << open_fd_count() << "\n";

    EXPECT_GT(welcomed.load(), 100);
    EXPECT_EQ(st.accepted, static_cast<std::uint64_t>(welcomed.load()));
    EXPECT_EQ(closed, st.accepted);
    EXPECT_EQ(open_fd_count(), fds_before);
    for (const auto& snap : server->snapshot()) {
        EXPECT_EQ(snap.state, SlotState::Free);
        EXPECT_EQ(snap.queue_depth, 0u);
    }

    // And the server still works.
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_NO_FATAL_FAILURE(start_streaming(c));
    c.pump(200ms);
    EXPECT_GT(c.udp_stats().frames_completed(), 0u);
}

TEST_F(RobustnessIT, RepeatedKillDash9LeavesNothingBehind) {
    cfg.frame_interval = 5ms;
    ASSERT_TRUE(start());
    std::this_thread::sleep_for(20ms);
    const int fds_before = open_fd_count();

    for (int i = 0; i < 30; ++i) {
        Child client(server->port(), {});
        ASSERT_TRUE(wait_until([&] { return streaming() == 1; }, 3000ms)) << "round " << i;
        client.signal(SIGKILL);
        ASSERT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 2000ms)) << "round " << i;
    }
    EXPECT_EQ(server->stats().accepted, 30u);
    EXPECT_EQ(open_fd_count(), fds_before);
}

// ---- slot reuse -------------------------------------------------------------

TEST_F(RobustnessIT, ReusedSlotStartsCleanAndOldClientGetsNothingMore) {
    cfg.frame_interval = 5ms;
    ASSERT_TRUE(start());

    TestClient a;
    ASSERT_NO_FATAL_FAILURE(connect_ok(a));
    ASSERT_NO_FATAL_FAILURE(start_streaming(a));
    a.pump(150ms);
    ASSERT_TRUE(a.send_line("QUIT"));
    ASSERT_EQ(a.read_reply(), "BYE");
    a.pump(50ms);  // drain what was in flight
    const auto a_after_quit = a.udp_stats().fragments();
    EXPECT_GT(a_after_quit, 0u);

    TestClient b;
    ASSERT_TRUE(b.connect(server->port()));
    const auto welcome = b.read_reply();
    ASSERT_TRUE(welcome);
    EXPECT_NE(welcome->find("slot=0"), std::string::npos) << *welcome;  // the same slot

    ASSERT_TRUE(b.send_line("REGISTER " + std::to_string(b.open_udp())));
    ASSERT_TRUE(b.read_reply());
    b.pump(150ms);
    EXPECT_EQ(b.udp_stats().fragments(), 0u) << "nothing before START";

    ASSERT_TRUE(b.send_line("STATUS"));
    const auto status = b.read_reply();
    ASSERT_TRUE(status);
    EXPECT_EQ(status_field(*status, "frames_sent"), 0u) << *status;  // A's counters are gone

    ASSERT_TRUE(b.send_line("START"));
    ASSERT_EQ(b.read_reply(), "OK STREAMING");
    b.pump(150ms);
    EXPECT_GT(b.udp_stats().frames_completed(), 0u);

    a.pump(50ms);
    EXPECT_EQ(a.udp_stats().fragments(), a_after_quit) << "A left: its port gets nothing more";
}

// Leaves A's oldest frames sitting in the queue when A goes. The generation check must
// keep them from reaching B, who takes over the same slot right away.
TEST_F(RobustnessIT, FramesQueuedForTheOldClientNeverReachTheNewOne) {
    make_frames_slow_to_send(cfg);
    cfg.queue_capacity = 8;
    cfg.queue_overflow = OverflowPolicy::DropNewest;  // the queue keeps the oldest frames
    ASSERT_TRUE(start());

    TestClient a;
    ASSERT_NO_FATAL_FAILURE(connect_ok(a));
    ASSERT_NO_FATAL_FAILURE(start_streaming(a));
    a.pump(300ms);
    ASSERT_TRUE(a.send_line("QUIT"));
    ASSERT_EQ(a.read_reply(), "BYE");

    TestClient b;
    ASSERT_NO_FATAL_FAILURE(connect_ok(b));
    UniqueFd udp(::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(udp.get(), reinterpret_cast<sockaddr*>(&addr), sizeof addr), 0);
    socklen_t len = sizeof addr;
    ::getsockname(udp.get(), reinterpret_cast<sockaddr*>(&addr), &len);
    timeval timeout{5, 0};
    ::setsockopt(udp.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);

    ASSERT_TRUE(b.send_line("REGISTER " + std::to_string(ntohs(addr.sin_port))));
    ASSERT_TRUE(b.read_reply());
    const auto produced_at_start = server->stats().frames_produced;
    ASSERT_TRUE(b.send_line("START"));

    std::uint8_t buf[2048];
    const ssize_t n = ::recv(udp.get(), buf, sizeof buf, 0);
    ASSERT_GT(n, 0);
    const auto header = parse_fragment(buf, static_cast<std::size_t>(n));
    ASSERT_TRUE(header);
    std::cout << "  frames produced before B's START: " << produced_at_start
              << ", B's first frame number: " << header->frame_seq << "\n";
    EXPECT_GE(header->frame_seq + 1, produced_at_start);
}

// ---- partial writes ---------------------------------------------------------

// A client that pauses reading (without reaching the stall limit) and then resumes must
// receive every reply, complete and in order: the send buffer fills, part of the output
// waits in the server, and POLLOUT has to finish the job.
TEST_F(RobustnessIT, RepliesSurviveAClientThatPausesReading) {
    cfg.max_commands_per_sec = 0;  // about output backlog, not about the rate limit
    cfg.max_line_bytes = 60000;
    cfg.heartbeat_interval = 10s;
    cfg.zombie_timeout = 30s;
    ASSERT_TRUE(start());

    TestClient c;
    ASSERT_TRUE(c.connect(server->port(), 4096));
    ASSERT_TRUE(wait_until([&] { return server->active_clients() == 1; }, 1000ms));

    const std::string padding(20000, 'x');
    int sent = 0;
    const auto deadline = SteadyClock::now() + 20s;
    while (server->snapshot()[0].pending_output_bytes == 0 && SteadyClock::now() < deadline) {
        ASSERT_TRUE(c.send_line("PING " + std::to_string(sent) + "-" + padding));
        ++sent;
        std::this_thread::sleep_for(2ms);  // let the server act on it before looking again
    }
    const auto pending = server->snapshot()[0].pending_output_bytes;
    std::cout << "  output backed up after " << sent << " replies: " << pending << " bytes waiting in the server\n";
    ASSERT_GT(pending, 0u) << "the test never reached the state it is about";

    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(server->stats().closed_by(CloseReason::Stalled), 0u);

    const auto welcome = c.read_reply();
    ASSERT_TRUE(welcome);
    ASSERT_EQ(welcome->rfind("WELCOME", 0), 0u);
    for (int i = 0; i < sent; ++i) {
        const auto reply = c.read_reply(5000ms);
        ASSERT_TRUE(reply) << "reply " << i;
        ASSERT_EQ(*reply, "PONG " + std::to_string(i) + "-" + padding) << "reply " << i;
    }
    EXPECT_TRUE(wait_until([&] { return server->snapshot()[0].pending_output_bytes == 0; }, 1000ms));
    EXPECT_EQ(server->active_clients(), 1u);
}

// ---- shutdown under load ----------------------------------------------------

TEST_F(RobustnessIT, StopUnderLoadNeverHangs) {
    for (int round = 0; round < 12; ++round) {
        Config c = cfg;
        if (round % 2 == 1) make_frames_slow_to_send(c);  // stop lands in the middle of a long frame
        Server s(c);
        std::string err;
        ASSERT_TRUE(s.start(&err)) << err;

        std::array<TestClient, 3> clients;
        for (auto& cl : clients) {
            ASSERT_TRUE(cl.connect(s.port()));
            ASSERT_TRUE(cl.read_reply());
            ASSERT_TRUE(cl.send_line("REGISTER " + std::to_string(cl.open_udp())));
            ASSERT_TRUE(cl.read_reply());
            ASSERT_TRUE(cl.send_line("START"));
            ASSERT_EQ(cl.read_reply(), "OK STREAMING");
        }
        std::this_thread::sleep_for(Ms(10 + round * 5));

        const auto t0 = SteadyClock::now();
        s.stop();
        const auto ms = std::chrono::duration_cast<Ms>(SteadyClock::now() - t0).count();
        EXPECT_LT(ms, 2000) << "round " << round;

        for (auto& cl : clients) {
            EXPECT_EQ(cl.read_reply(), "BYE SHUTDOWN") << "round " << round;
            EXPECT_TRUE(cl.wait_eof(1000ms)) << "round " << round;
        }
    }
}

TEST_F(RobustnessIT, StopIsSafeWhenRepeatedOrWithoutStart) {
    Server never_started(cfg);
    never_started.stop();

    ASSERT_TRUE(start());
    server->stop();
    server->stop();
}

// ---- many clients competing -------------------------------------------------

TEST_F(RobustnessIT, TenReconnectingClientsShareThreeSlots) {
    cfg.frame_interval = 5ms;
    ASSERT_TRUE(start());

    std::vector<std::unique_ptr<Child>> clients;
    for (int i = 0; i < 10; ++i)
        clients.push_back(std::make_unique<Child>(
            server->port(), std::vector<std::string>{"--reconnect-min-ms", "20", "--reconnect-max-ms", "100"}));

    ASSERT_TRUE(wait_until([&] { return streaming() == 3; }, 5000ms));
    ASSERT_TRUE(wait_until([&] { return server->stats().rejected_full > 10; }, 5000ms));
    EXPECT_EQ(server->active_clients(), 3u);

    // Waiting clients must take over as clients go, and the limit must hold throughout.
    while (clients.size() > 3) {
        clients.pop_back();
        EXPECT_TRUE(wait_until([&] { return streaming() == 3; }, 3000ms)) << clients.size() << " left";
        EXPECT_LE(server->active_clients(), 3u);
    }
    clients.clear();
    EXPECT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 2000ms));
}

// ---- one IO thread must stay fair ------------------------------------------

namespace {

// Pings for `duration` and returns the slowest answer.
Ms worst_ping_time(TestClient& c, Ms duration) {
    Ms worst{0};
    const auto end = SteadyClock::now() + duration;
    while (SteadyClock::now() < end) {
        const auto t0 = SteadyClock::now();
        if (!c.send_line("PING rtt")) return Ms(999999);
        const auto reply = c.read_reply(3000ms);
        if (reply != "PONG rtt") return Ms(999999);
        worst = std::max(worst, std::chrono::duration_cast<Ms>(SteadyClock::now() - t0));
        std::this_thread::sleep_for(5ms);
    }
    return worst;
}

}  // namespace

// Rejected clients reconnecting as fast as they can must not keep the IO thread in its
// accept loop: the client already connected still needs answers and heartbeats.
TEST_F(RobustnessIT, FloodOfRejectedConnectionsDoesNotStarveExistingClients) {
    cfg.max_clients = 1;
    cfg.heartbeat_interval = 100ms;
    cfg.zombie_timeout = 1000ms;
    ASSERT_TRUE(start());
    TestClient healthy;
    ASSERT_NO_FATAL_FAILURE(connect_ok(healthy));

    std::atomic<bool> done{false};
    std::atomic<int> attempts{0};
    std::vector<std::thread> flood;
    for (int t = 0; t < 4; ++t)
        flood.emplace_back([&] {
            while (!done && attempts.fetch_add(1) < 12000) {
                TestClient c;
                if (c.connect(server->port())) c.read_reply(200ms);
            }
        });
    const Ms worst = worst_ping_time(healthy, 1000ms);
    done = true;
    for (auto& t : flood) t.join();

    // Attempts stop at the cap (12000) if the flood is faster than the 1 s measurement.
    std::cout << "  " << attempts.load() << " connection attempts, rejected=" << server->stats().rejected_full
              << ", worst ping " << worst.count() << " ms, heartbeats=" << healthy.heartbeats() << "\n";
    EXPECT_LT(worst.count(), 200);
    EXPECT_GE(healthy.heartbeats(), 5u);
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 0u);
    EXPECT_EQ(server->active_clients(), 1u);
}

// Same for a client sending commands as fast as it can.
// The limit is off here: fairness of the IO thread has to hold on its own.
TEST_F(RobustnessIT, FloodingClientDoesNotStarveOtherClients) {
    cfg.max_commands_per_sec = 0;
    cfg.heartbeat_interval = 100ms;
    cfg.zombie_timeout = 1000ms;
    ASSERT_TRUE(start());
    TestClient healthy, flooder;
    ASSERT_NO_FATAL_FAILURE(connect_ok(healthy));
    ASSERT_NO_FATAL_FAILURE(connect_ok(flooder));

    std::atomic<bool> done{false};
    std::atomic<std::uint64_t> flooded{0};
    std::thread flood([&] {
        std::string batch;
        for (int i = 0; i < 500; ++i) batch += "PING f\n";
        char sink[65536];
        while (!done) {
            const ssize_t n = ::send(flooder.tcp_fd(), batch.data(), batch.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n > 0) flooded += static_cast<std::uint64_t>(n);
            while (::recv(flooder.tcp_fd(), sink, sizeof sink, MSG_DONTWAIT) > 0) {
            }
        }
    });
    const Ms worst = worst_ping_time(healthy, 1000ms);
    done = true;
    flood.join();

    std::cout << "  flooder sent " << flooded.load() / 1024 << " KiB, worst ping " << worst.count()
              << " ms, heartbeats=" << healthy.heartbeats() << "\n";
    EXPECT_LT(worst.count(), 200);
    EXPECT_GE(healthy.heartbeats(), 5u);
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 0u);
}

// ---- command rate limit -----------------------------------------------------

// The flooder only gets its allowance, stays connected, and is not mistaken for a zombie.
TEST_F(RobustnessIT, FloodIsLimitedToTheConfiguredRate) {
    cfg.max_commands_per_sec = 100;
    cfg.heartbeat_interval = 100ms;
    cfg.zombie_timeout = 500ms;  // shorter than the flood: throttling must not look like silence
    ASSERT_TRUE(start());
    TestClient flooder;
    ASSERT_NO_FATAL_FAILURE(connect_ok(flooder));

    std::string batch;
    for (int i = 0; i < 500; ++i) batch += "PING f\n";
    std::string received;
    char sink[65536];
    const auto t0 = SteadyClock::now();
    while (SteadyClock::now() - t0 < 1500ms) {
        ::send(flooder.tcp_fd(), batch.data(), batch.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
        ssize_t n;
        while ((n = ::recv(flooder.tcp_fd(), sink, sizeof sink, MSG_DONTWAIT)) > 0)
            received.append(sink, static_cast<std::size_t>(n));
        std::this_thread::sleep_for(1ms);
    }

    std::size_t pongs = 0;
    for (auto pos = received.find("PONG f\n"); pos != std::string::npos; pos = received.find("PONG f\n", pos + 1))
        ++pongs;
    const auto snap = server->snapshot()[0];
    std::cout << "  answered " << pongs << " commands in 1.5 s, throttled " << snap.throttle_events << " times\n";

    EXPECT_GE(pongs, 200u);  // burst of 100 + 150 at the rate, give or take the scheduler
    EXPECT_LE(pongs, 260u);
    EXPECT_GT(snap.throttle_events, 0u);
    EXPECT_EQ(server->active_clients(), 1u);
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 0u);
}

// The limit applies to the TCP commands only. The track stream is a separate path, so
// even the client being throttled keeps receiving every frame, on time.
TEST_F(RobustnessIT, ThrottledClientKeepsReceivingEveryTrackFrame) {
    cfg.max_commands_per_sec = 100;
    ASSERT_TRUE(start());
    TestClient flooder, quiet;
    ASSERT_NO_FATAL_FAILURE(connect_ok(flooder));
    ASSERT_NO_FATAL_FAILURE(connect_ok(quiet));
    ASSERT_NO_FATAL_FAILURE(start_streaming(flooder));
    ASSERT_NO_FATAL_FAILURE(start_streaming(quiet));

    std::string batch;
    for (int i = 0; i < 500; ++i) batch += "PING f\n";
    std::thread other([&] { quiet.pump(2000ms); });
    const auto t0 = SteadyClock::now();
    while (SteadyClock::now() - t0 < 2000ms) {
        ::send(flooder.tcp_fd(), batch.data(), batch.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
        flooder.pump(5ms);
    }
    other.join();

    const auto snap = server->snapshot();
    const auto& fs = flooder.stream_stats();
    const auto& qs = quiet.stream_stats();
    std::cout << "  throttled " << snap[0].throttle_events << " times | flooder: frames=" << fs.received()
              << " lost=" << fs.lost() << " latency max=" << fs.latency_max_us() / 1000.0 << " ms | quiet: frames="
              << qs.received() << " lost=" << qs.lost() << " | period max="
              << server->stats().frame_period.max_us() / 1000.0 << " ms\n";

    EXPECT_GT(snap[0].throttle_events, 0u);
    EXPECT_EQ(snap[0].frames_dropped, 0u);
    EXPECT_EQ(fs.lost(), 0u);
    EXPECT_EQ(qs.lost(), 0u);
    EXPECT_GE(fs.received(), 50u);  // about 57 frames in 2 s at 35 ms
    EXPECT_NEAR(static_cast<double>(fs.received()), static_cast<double>(qs.received()), 3.0);
}

TEST_F(RobustnessIT, BurstWithinTheLimitIsNotThrottled) {
    cfg.max_commands_per_sec = 100;
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));

    std::string burst;
    for (int i = 0; i < 80; ++i) burst += "PING " + std::to_string(i) + "\n";
    ASSERT_TRUE(c.send_raw(burst));
    for (int i = 0; i < 80; ++i) ASSERT_EQ(c.read_reply(), "PONG " + std::to_string(i));
    EXPECT_EQ(server->snapshot()[0].throttle_events, 0u);
}

// Throttling delays commands; it never loses or reorders them, and QUIT still works.
TEST_F(RobustnessIT, ThrottledCommandsAreAllAnsweredInOrder) {
    cfg.max_commands_per_sec = 100;
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));

    std::string burst;
    for (int i = 0; i < 150; ++i) burst += "PING " + std::to_string(i) + "\n";
    burst += "QUIT\n";
    ASSERT_TRUE(c.send_raw(burst));

    const auto t0 = SteadyClock::now();
    for (int i = 0; i < 150; ++i) ASSERT_EQ(c.read_reply(), "PONG " + std::to_string(i));
    EXPECT_EQ(c.read_reply(), "BYE");
    const auto ms = std::chrono::duration_cast<Ms>(SteadyClock::now() - t0).count();
    std::cout << "  151 commands took " << ms << " ms\n";
    EXPECT_GE(ms, 400);  // 51 above the burst at 10 ms each
    EXPECT_TRUE(c.wait_eof(1000ms));
}

TEST_F(RobustnessIT, OneClientsLimitDoesNotAffectAnother) {
    cfg.max_commands_per_sec = 100;
    ASSERT_TRUE(start());
    TestClient flooder, other;
    ASSERT_NO_FATAL_FAILURE(connect_ok(flooder));
    ASSERT_NO_FATAL_FAILURE(connect_ok(other));

    std::string burst;
    for (int i = 0; i < 1000; ++i) burst += "PING f\n";
    ASSERT_TRUE(flooder.send_raw(burst));
    ASSERT_TRUE(wait_until([&] { return server->snapshot()[0].throttle_events > 0; }, 1000ms));

    for (int i = 0; i < 50; ++i) ASSERT_TRUE(other.send_line("PING o"));
    for (int i = 0; i < 50; ++i) ASSERT_EQ(other.read_reply(), "PONG o");
    EXPECT_EQ(server->snapshot()[1].throttle_events, 0u);
}

TEST_F(RobustnessIT, ThrottledClientThatDisconnectsIsStillReleased) {
    cfg.max_commands_per_sec = 10;
    cfg.heartbeat_interval = 1000ms;
    cfg.zombie_timeout = 30s;
    ASSERT_TRUE(start());
    {
        TestClient c;
        ASSERT_NO_FATAL_FAILURE(connect_ok(c));
        std::string burst;
        for (int i = 0; i < 500; ++i) burst += "PING x\n";
        ASSERT_TRUE(c.send_raw(burst));
        ASSERT_TRUE(wait_until([&] { return server->snapshot()[0].throttle_events > 0; }, 1000ms));
    }
    // It may take a moment to get through what it already sent, but not the zombie timeout.
    EXPECT_TRUE(wait_until([&] { return server->active_clients() == 0; }, 5000ms));
    EXPECT_EQ(server->stats().closed_by(CloseReason::Timeout), 0u);
}

// ---- input edge cases -------------------------------------------------------

TEST_F(RobustnessIT, CommandSentOneByteAtATime) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    for (const char ch : std::string("PING split\n")) {
        ASSERT_TRUE(c.send_raw(std::string(1, ch)));
        std::this_thread::sleep_for(2ms);
    }
    EXPECT_EQ(c.read_reply(), "PONG split");
}

TEST_F(RobustnessIT, SeveralCommandsInOnePacketAnswerInOrder) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_TRUE(c.send_raw("PING 1\nPING 2\r\n\n\nPING 3\n"));
    EXPECT_EQ(c.read_reply(), "PONG 1");
    EXPECT_EQ(c.read_reply(), "PONG 2");
    EXPECT_EQ(c.read_reply(), "PONG 3");
}

TEST_F(RobustnessIT, NothingAfterQuitIsProcessed) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_TRUE(c.send_raw("QUIT\nPING late\n"));
    EXPECT_EQ(c.read_reply(), "BYE");
    EXPECT_TRUE(c.wait_eof(1000ms));
    EXPECT_FALSE(c.read_reply(50ms).has_value());
}

TEST_F(RobustnessIT, BinaryGarbageIsRejectedAndTheConnectionSurvives) {
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_TRUE(c.send_raw(std::string("\x01\x02\xff\xfe", 4) + std::string(1, '\0') + "abc\n"));
    const auto r = c.read_reply();
    ASSERT_TRUE(r);
    EXPECT_EQ(r->rfind("ERR UNKNOWN_COMMAND", 0), 0u) << *r;
    ASSERT_TRUE(c.send_line("PING alive"));
    EXPECT_EQ(c.read_reply(), "PONG alive");
}

TEST_F(RobustnessIT, LineLengthLimitIsExact) {
    cfg.max_line_bytes = 64;
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));

    const std::string fits = "PING " + std::string(59, 'x');
    ASSERT_EQ(fits.size(), 64u);
    ASSERT_TRUE(c.send_line(fits));
    EXPECT_EQ(c.read_reply(), "PONG " + std::string(59, 'x'));

    ASSERT_TRUE(c.send_line(fits + "x"));
    EXPECT_EQ(c.read_reply(), "ERR LINE_TOO_LONG");
    EXPECT_TRUE(c.wait_eof(1000ms));
}

// Commands that arrived before an overlong line in the same packet were valid: they
// are answered, and an overlong line that has its newline is never run as a command.
TEST_F(RobustnessIT, ValidCommandsBeforeAnOverlongLineStillRun) {
    cfg.max_line_bytes = 64;
    ASSERT_TRUE(start());
    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_TRUE(c.send_raw("PING first\nPING " + std::string(200, 'y') + "\nPING after\n"));
    EXPECT_EQ(c.read_reply(), "PONG first");
    EXPECT_EQ(c.read_reply(), "ERR LINE_TOO_LONG");
    EXPECT_TRUE(c.wait_eof(1000ms));
    EXPECT_FALSE(c.read_reply(50ms).has_value());
}

// ---- UDP path ---------------------------------------------------------------

TEST_F(RobustnessIT, StreamingToAPortNobodyListensOnKeepsTheServerAlive) {
    cfg.frame_interval = 5ms;
    ASSERT_TRUE(start());

    std::uint16_t dead_port;
    {
        UniqueFd s(::socket(AF_INET, SOCK_DGRAM, 0));
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ::bind(s.get(), reinterpret_cast<sockaddr*>(&a), sizeof a);
        socklen_t len = sizeof a;
        ::getsockname(s.get(), reinterpret_cast<sockaddr*>(&a), &len);
        dead_port = ntohs(a.sin_port);
    }

    TestClient c;
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
    ASSERT_TRUE(c.send_line("REGISTER " + std::to_string(dead_port)));
    ASSERT_TRUE(c.read_reply());
    ASSERT_TRUE(c.send_line("START"));
    ASSERT_EQ(c.read_reply(), "OK STREAMING");
    c.pump(300ms);

    const auto snap = server->snapshot()[0];
    std::cout << "  frames_sent=" << snap.frames_sent << " send_errors=" << snap.send_errors << "\n";
    EXPECT_GT(snap.frames_sent, 10u);
    ASSERT_TRUE(c.send_line("PING alive"));
    EXPECT_EQ(c.read_reply(), "PONG alive");
}

TEST_F(RobustnessIT, RegisterWhileStreamingMovesTheStream) {
    cfg.frame_interval = 5ms;
    ASSERT_TRUE(start());

    TestClient old_port, control;
    ASSERT_NO_FATAL_FAILURE(connect_ok(old_port));
    ASSERT_NO_FATAL_FAILURE(start_streaming(old_port));
    old_port.pump(100ms);
    const auto before_move = old_port.udp_stats().fragments();
    EXPECT_GT(before_move, 0u);

    // `control` only provides a second UDP socket to move the stream to.
    const auto new_port = control.open_udp();
    ASSERT_TRUE(old_port.send_line("REGISTER " + std::to_string(new_port)));
    const auto reply = old_port.read_reply();
    ASSERT_TRUE(reply);
    EXPECT_EQ(reply->rfind("OK REGISTERED", 0), 0u) << *reply;
    old_port.pump(30ms);  // at most what was already in flight
    const auto settled = old_port.udp_stats().fragments();

    control.pump(200ms);
    EXPECT_GT(control.udp_stats().frames_completed(), 10u);
    old_port.pump(100ms);
    EXPECT_EQ(old_port.udp_stats().fragments(), settled);
}

// ---- configuration limits ---------------------------------------------------

TEST_F(RobustnessIT, PortAlreadyInUseIsAReportedStartFailure) {
    ASSERT_TRUE(start());
    Config second = cfg;
    second.tcp_port = server->port();
    Server other(second);
    std::string err;
    EXPECT_FALSE(other.start(&err));
    EXPECT_NE(err.find("bind"), std::string::npos) << err;

    TestClient c;  // the first server is not disturbed
    ASSERT_NO_FATAL_FAILURE(connect_ok(c));
}

TEST_F(RobustnessIT, LargestAllowedSlotCountWorks) {
    cfg.max_clients = 64;
    cfg.heartbeat_interval = 200ms;
    cfg.zombie_timeout = 30s;
    ASSERT_TRUE(start());

    std::vector<std::unique_ptr<TestClient>> clients;
    for (int i = 0; i < 64; ++i) {
        clients.push_back(std::make_unique<TestClient>());
        ASSERT_NO_FATAL_FAILURE(connect_ok(*clients.back())) << "client " << i;
    }
    EXPECT_EQ(server->active_clients(), 64u);

    TestClient extra;
    ASSERT_TRUE(extra.connect(server->port()));
    EXPECT_EQ(extra.read_reply(), "ERR FULL");

    ASSERT_NO_FATAL_FAILURE(start_streaming(*clients[63]));
    clients[63]->pump(200ms);
    EXPECT_GT(clients[63]->udp_stats().frames_completed(), 0u);
}
