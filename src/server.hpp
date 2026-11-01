#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "config.hpp"
#include "interval_stats.hpp"
#include "liveness.hpp"
#include "slot.hpp"
#include "socket_util.hpp"
#include "track_frame.hpp"

namespace radar {

struct ServerStats {
    std::uint64_t accepted = 0;
    std::uint64_t rejected_full = 0;
    std::uint64_t frames_produced = 0;
    std::uint64_t frames_dropped = 0;        // queue overflow, summed over all clients
    std::uint64_t stale_frames_skipped = 0;  // dequeued after STOP / slot reuse
    std::uint64_t producer_overruns = 0;     // producer missed its 35 ms deadline
    std::array<std::uint64_t, kCloseReasonCount> closed{};
    IntervalStats frame_period;  // measured time between frames, since start

    std::uint64_t closed_by(CloseReason r) const { return closed[static_cast<std::size_t>(r)]; }
};

struct SlotSnapshot {
    std::size_t index = 0;
    SlotState state = SlotState::Free;
    std::string peer;
    std::uint16_t udp_port = 0;
    std::uint64_t frames_sent = 0;
    std::uint64_t fragments_sent = 0;
    std::uint64_t frames_dropped = 0;
    std::uint64_t send_errors = 0;
    std::size_t queue_depth = 0;
    std::size_t pending_output_bytes = 0;  // TCP replies the client has not taken yet
};

// The periodic stats log as text: one line for the server, one per occupied slot.
// `window` is the frame period measured since the previous report.
std::vector<std::string> format_stats_report(const ServerStats& stats, const std::vector<SlotSnapshot>& slots,
                                             const IntervalStats& window);

// Multi-client track server.
//
// Threads (see docs/DESIGN.md):
//   io        : poll() loop - accept, TCP commands, heartbeats, zombie sweep.
//               The only thread that touches TCP sockets.
//   producer  : every frame_interval builds one frame and pushes it to the
//               bounded queue of every STREAMING slot (fan-out, shared_ptr).
//   sender[i] : one per slot, pops its queue and sends UDP fragments.
class Server {
public:
    explicit Server(Config cfg);
    ~Server();
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    bool start(std::string* error);
    void stop();  // graceful: sends "BYE SHUTDOWN", joins every thread. Idempotent.

    std::uint16_t port() const { return bound_port_; }
    std::size_t fragments_per_frame() const { return fragments_per_frame_; }
    std::size_t active_clients() const { return active_.load(); }
    std::vector<SlotSnapshot> snapshot() const;
    ServerStats stats() const;

    // Test seam: simulates a stuck consumer. Slot `index`'s sender cannot start its next
    // frame until the returned lock is released. Do not send STOP or close that slot
    // meanwhile: the io thread waits for the sender at those points.
    std::unique_lock<std::mutex> hold_sender_for_test(std::size_t index);

private:
    struct FrameItem {
        std::shared_ptr<const PreparedFrame> frame;
        std::uint64_t generation;  // slot generation at enqueue time
    };
    struct Slot;

    void io_loop();
    void producer_loop();
    void sender_loop(std::size_t index);

    void handle_accept(Clock::time_point now);
    void handle_readable(std::size_t index, Clock::time_point now);
    void handle_line(std::size_t index, const std::string& line);
    void housekeeping(Clock::time_point now);
    void log_stats_report();
    int poll_timeout_ms(Clock::time_point now) const;

    bool queue_line(std::size_t index, const std::string& line);
    bool flush(std::size_t index);
    void close_slot(std::size_t index, CloseReason reason, const char* bye = nullptr);
    std::string status_line(std::size_t index) const;
    void wake();

    Config cfg_;
    std::uint16_t bound_port_ = 0;
    std::size_t fragments_per_frame_ = 0;

    UniqueFd listen_fd_;
    UniqueFd wake_rd_, wake_wr_;
    std::vector<std::unique_ptr<Slot>> slots_;
    std::vector<UniqueFd> udp_fds_;  // one per sender thread

    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};
    std::mutex producer_mtx_;
    std::condition_variable producer_cv_;

    std::thread io_thread_;
    std::thread producer_thread_;
    std::vector<std::thread> sender_threads_;

    std::atomic<std::size_t> active_{0};
    std::atomic<std::uint64_t> accepted_{0}, rejected_full_{0}, frames_produced_{0}, frames_dropped_{0},
        stale_skipped_{0}, producer_overruns_{0};
    std::array<std::atomic<std::uint64_t>, kCloseReasonCount> closed_{};

    mutable std::mutex period_mtx_;
    IntervalStats period_total_, period_window_;

    Clock::time_point next_stats_report_{};  // io thread only
};

}  // namespace radar
