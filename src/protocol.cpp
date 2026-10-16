#include "protocol.hpp"

#include <cctype>
#include <vector>

namespace radar {

namespace {

std::vector<std::string> split_ws(std::string_view s) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
        std::size_t j = i;
        while (j < s.size() && !std::isspace(static_cast<unsigned char>(s[j]))) ++j;
        if (j > i) out.emplace_back(s.substr(i, j - i));
        i = j;
    }
    return out;
}

std::string upper(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

bool parse_port(const std::string& s, std::uint16_t& out) {
    if (s.empty() || s.size() > 5) return false;
    unsigned long v = 0;
    for (char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
        v = v * 10 + static_cast<unsigned long>(c - '0');
    }
    if (v == 0 || v > 65535) return false;
    out = static_cast<std::uint16_t>(v);
    return true;
}

Command invalid(std::string why) {
    Command c;
    c.type = CommandType::Invalid;
    c.error = std::move(why);
    return c;
}

}  // namespace

Command parse_command(std::string_view line) {
    const auto tok = split_ws(line);
    Command cmd;
    if (tok.empty()) return cmd;  // Empty

    const std::string kw = upper(tok[0]);
    const std::size_t nargs = tok.size() - 1;

    if (kw == "REGISTER") {
        if (nargs != 1) return invalid("usage: REGISTER <udp_port>");
        if (!parse_port(tok[1], cmd.udp_port)) return invalid("udp_port must be 1..65535");
        cmd.type = CommandType::Register;
        return cmd;
    }
    if (kw == "PING" || kw == "PONG") {
        if (nargs > 1) return invalid("usage: " + kw + " [token]");
        cmd.type = kw == "PING" ? CommandType::Ping : CommandType::Pong;
        if (nargs == 1) cmd.arg = tok[1];
        return cmd;
    }

    CommandType simple = CommandType::Unknown;
    if (kw == "START") simple = CommandType::Start;
    else if (kw == "STOP") simple = CommandType::Stop;
    else if (kw == "STATUS") simple = CommandType::Status;
    else if (kw == "QUIT") simple = CommandType::Quit;

    if (simple == CommandType::Unknown) {
        cmd.type = CommandType::Unknown;
        cmd.arg = tok[0];
        return cmd;
    }
    if (nargs != 0) return invalid(kw + " takes no arguments");
    cmd.type = simple;
    return cmd;
}

const char* to_string(CommandType type) {
    switch (type) {
        case CommandType::Empty: return "EMPTY";
        case CommandType::Register: return "REGISTER";
        case CommandType::Start: return "START";
        case CommandType::Stop: return "STOP";
        case CommandType::Ping: return "PING";
        case CommandType::Pong: return "PONG";
        case CommandType::Status: return "STATUS";
        case CommandType::Quit: return "QUIT";
        case CommandType::Unknown: return "UNKNOWN";
        case CommandType::Invalid: return "INVALID";
    }
    return "?";
}

}  // namespace radar
