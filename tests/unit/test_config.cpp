#include <gtest/gtest.h>

#include <sstream>

#include "config.hpp"

using namespace radar;
using namespace std::chrono_literals;

namespace {
std::optional<Config> parse(const std::string& text, std::string* err = nullptr) {
    std::istringstream in(text);
    return parse_config(in, err);
}
}  // namespace

TEST(Config, EmptyFileGivesDefaults) {
    auto c = parse("");
    ASSERT_TRUE(c);
    EXPECT_EQ(c->tcp_port, 8088);
    EXPECT_EQ(c->max_clients, 3u);
    EXPECT_EQ(c->frame_interval, 35ms);
    EXPECT_EQ(c->queue_overflow, OverflowPolicy::DropOldest);
}

TEST(Config, FrameMustFitInTheFragmentCountField) {
    std::string err;
    // 1 payload byte per datagram: the fragment count equals the frame size.
    EXPECT_TRUE(parse("tracks_per_frame = 2729\nmax_datagram_bytes = 21\n", &err)) << err;  // 65512
    EXPECT_FALSE(parse("tracks_per_frame = 2730\nmax_datagram_bytes = 21\n", &err));        // 65536
    EXPECT_NE(err.find("fragments"), std::string::npos) << err;
    EXPECT_FALSE(parse("tracks_per_frame = 10000\nmax_datagram_bytes = 21\n"));
    EXPECT_TRUE(parse("tracks_per_frame = 10000\nmax_datagram_bytes = 512\n"));
}

TEST(Config, ParsesKeysCommentsAndWhitespace) {
    auto c = parse(
        "# comment\n"
        "tcp_port = 9000\n"
        "  max_clients=5   # trailing comment\n"
        "\n"
        "heartbeat_interval_ms = 200\n"
        "zombie_timeout_ms = 700\n"
        "queue_capacity = 2\n"
        "queue_overflow = drop_newest\n"
        "log_level = warn\n");
    ASSERT_TRUE(c);
    EXPECT_EQ(c->tcp_port, 9000);
    EXPECT_EQ(c->max_clients, 5u);
    EXPECT_EQ(c->heartbeat_interval, 200ms);
    EXPECT_EQ(c->zombie_timeout, 700ms);
    EXPECT_EQ(c->queue_capacity, 2u);
    EXPECT_EQ(c->queue_overflow, OverflowPolicy::DropNewest);
    EXPECT_EQ(c->log_level, LogLevel::Warn);
}

TEST(Config, UnknownKeyReportsLine) {
    std::string err;
    EXPECT_FALSE(parse("tcp_port = 1\nmax_client = 3\n", &err));
    EXPECT_NE(err.find("line 2"), std::string::npos);
    EXPECT_NE(err.find("max_client"), std::string::npos);
}

TEST(Config, RejectsOutOfRangeAndGarbage) {
    EXPECT_FALSE(parse("tcp_port = 70000\n"));
    EXPECT_FALSE(parse("max_clients = 0\n"));
    EXPECT_FALSE(parse("max_clients = three\n"));
    EXPECT_FALSE(parse("bind_address = localhost\n"));
    EXPECT_FALSE(parse("queue_overflow = block\n"));
    EXPECT_FALSE(parse("just some text\n"));
}

TEST(Config, ZombieTimeoutMustExceedHeartbeat) {
    std::string err;
    EXPECT_FALSE(parse("heartbeat_interval_ms = 1000\nzombie_timeout_ms = 1000\n", &err));
    EXPECT_NE(err.find("zombie_timeout_ms"), std::string::npos);
}

TEST(Config, MissingFile) {
    std::string err;
    EXPECT_FALSE(load_config_file("/nonexistent/radar.conf", &err));
    EXPECT_NE(err.find("cannot open"), std::string::npos);
}
