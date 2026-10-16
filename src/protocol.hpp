#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace radar {

// Client -> server commands (one per line, '\n' terminated, keyword case-insensitive).
//   REGISTER <udp_port>   send track fragments to <tcp peer ip>:<udp_port>
//   START                 begin streaming (requires REGISTER)
//   STOP                  pause streaming
//   PING [token]          server answers "PONG [token]"
//   PONG [seq]            answer to a server "HB <seq>" heartbeat
//   STATUS                per-slot counters
//   QUIT                  graceful close, server answers "BYE"
enum class CommandType { Empty, Register, Start, Stop, Ping, Pong, Status, Quit, Unknown, Invalid };

struct Command {
    CommandType type = CommandType::Empty;
    std::uint16_t udp_port = 0;  // REGISTER
    std::string arg;             // PING/PONG token, or the unknown keyword
    std::string error;           // Invalid: why
};

Command parse_command(std::string_view line);

const char* to_string(CommandType type);

}  // namespace radar
