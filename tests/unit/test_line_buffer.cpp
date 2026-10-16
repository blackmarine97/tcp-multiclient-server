#include <gtest/gtest.h>

#include <string>

#include "line_buffer.hpp"

using namespace radar;

namespace {
bool feed(LineBuffer& lb, const std::string& s) { return lb.append(s.data(), s.size()); }
}  // namespace

TEST(LineBuffer, SplitsMultipleLinesInOneChunk) {
    LineBuffer lb;
    ASSERT_TRUE(feed(lb, "START\nSTOP\nQUIT\n"));
    EXPECT_EQ(*lb.next_line(), "START");
    EXPECT_EQ(*lb.next_line(), "STOP");
    EXPECT_EQ(*lb.next_line(), "QUIT");
    EXPECT_FALSE(lb.next_line().has_value());
}

TEST(LineBuffer, ReassemblesLineSplitAcrossReads) {
    LineBuffer lb;
    feed(lb, "REGI");
    EXPECT_FALSE(lb.next_line().has_value());
    feed(lb, "STER 50");
    feed(lb, "00\n");
    EXPECT_EQ(*lb.next_line(), "REGISTER 5000");
}

TEST(LineBuffer, StripsCarriageReturn) {
    LineBuffer lb;
    feed(lb, "PING\r\n");
    EXPECT_EQ(*lb.next_line(), "PING");
}

TEST(LineBuffer, EmptyLine) {
    LineBuffer lb;
    feed(lb, "\n");
    EXPECT_EQ(*lb.next_line(), "");
}

TEST(LineBuffer, OverflowWithoutNewline) {
    LineBuffer lb(8);
    EXPECT_TRUE(feed(lb, "12345678"));
    EXPECT_FALSE(feed(lb, "9"));
    EXPECT_TRUE(lb.overflowed());
}

TEST(LineBuffer, LongStreamOfShortLinesIsFine) {
    LineBuffer lb(8);
    std::string many;
    for (int i = 0; i < 100; ++i) many += "PONG 1\n";
    EXPECT_TRUE(feed(lb, many));
    int n = 0;
    while (lb.next_line()) ++n;
    EXPECT_EQ(n, 100);
}

TEST(LineBuffer, ClearResetsOverflow) {
    LineBuffer lb(4);
    feed(lb, "toolong");
    ASSERT_TRUE(lb.overflowed());
    lb.clear();
    EXPECT_FALSE(lb.overflowed());
    EXPECT_TRUE(feed(lb, "ok\n"));
    EXPECT_EQ(*lb.next_line(), "ok");
}
