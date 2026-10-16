#pragma once

#include <cstddef>

namespace radar {

// Slot lifecycle (see docs/DESIGN.md for the state diagram):
//   Free -> Connected -> Registered <-> Streaming
//   any active state -> Closing -> Free
enum class SlotState { Free, Connected, Registered, Streaming, Closing };

enum class CloseReason {
    Quit,        // client sent QUIT
    PeerClosed,  // recv() == 0 (FIN): normal close or killed process
    Reset,       // socket error (ECONNRESET, EPIPE, ...)
    Timeout,     // zombie: nothing received for zombie_timeout
    Stalled,     // zombie: client stopped reading, output backlog overflowed
    Protocol,    // line too long
    Shutdown,    // server is stopping
};
inline constexpr std::size_t kCloseReasonCount = 7;

inline const char* to_string(SlotState s) {
    switch (s) {
        case SlotState::Free: return "FREE";
        case SlotState::Connected: return "CONNECTED";
        case SlotState::Registered: return "REGISTERED";
        case SlotState::Streaming: return "STREAMING";
        case SlotState::Closing: return "CLOSING";
    }
    return "?";
}

inline const char* to_string(CloseReason r) {
    switch (r) {
        case CloseReason::Quit: return "quit";
        case CloseReason::PeerClosed: return "peer-closed";
        case CloseReason::Reset: return "reset";
        case CloseReason::Timeout: return "zombie-timeout";
        case CloseReason::Stalled: return "zombie-stalled";
        case CloseReason::Protocol: return "protocol-error";
        case CloseReason::Shutdown: return "shutdown";
    }
    return "?";
}

}  // namespace radar
