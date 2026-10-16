#include <gtest/gtest.h>

#include "protocol.hpp"

using namespace radar;

TEST(Protocol, Register) {
    auto c = parse_command("REGISTER 5000");
    EXPECT_EQ(c.type, CommandType::Register);
    EXPECT_EQ(c.udp_port, 5000);
}

TEST(Protocol, KeywordsAreCaseInsensitiveAndWhitespaceTolerant) {
    EXPECT_EQ(parse_command("  start  ").type, CommandType::Start);
    EXPECT_EQ(parse_command("Stop").type, CommandType::Stop);
    EXPECT_EQ(parse_command("register\t6000").udp_port, 6000);
}

TEST(Protocol, RegisterRejectsBadPorts) {
    EXPECT_EQ(parse_command("REGISTER").type, CommandType::Invalid);
    EXPECT_EQ(parse_command("REGISTER 0").type, CommandType::Invalid);
    EXPECT_EQ(parse_command("REGISTER 65536").type, CommandType::Invalid);
    EXPECT_EQ(parse_command("REGISTER abc").type, CommandType::Invalid);
    EXPECT_EQ(parse_command("REGISTER 1 2").type, CommandType::Invalid);
    EXPECT_EQ(parse_command("REGISTER 65535").type, CommandType::Register);
}

TEST(Protocol, PingPongCarryOptionalToken) {
    auto p = parse_command("PING abc");
    EXPECT_EQ(p.type, CommandType::Ping);
    EXPECT_EQ(p.arg, "abc");
    EXPECT_EQ(parse_command("PONG").type, CommandType::Pong);
    EXPECT_EQ(parse_command("PONG 17").arg, "17");
    EXPECT_EQ(parse_command("PONG 1 2").type, CommandType::Invalid);
}

TEST(Protocol, NoArgCommandsRejectArguments) {
    EXPECT_EQ(parse_command("QUIT now").type, CommandType::Invalid);
    EXPECT_EQ(parse_command("STATUS").type, CommandType::Status);
    EXPECT_EQ(parse_command("QUIT").type, CommandType::Quit);
}

TEST(Protocol, UnknownAndEmpty) {
    auto u = parse_command("FIRE");
    EXPECT_EQ(u.type, CommandType::Unknown);
    EXPECT_EQ(u.arg, "FIRE");
    EXPECT_EQ(parse_command("").type, CommandType::Empty);
    EXPECT_EQ(parse_command("   ").type, CommandType::Empty);
}
