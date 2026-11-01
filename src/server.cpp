#include "server.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <optional>

#include "bounded_queue.hpp"
#include "line_buffer.hpp"
#include "log.hpp"
#include "protocol.hpp"

namespace radar {

namespace {

constexpr std::size_t kMaxOutbuf = 64 * 1024;  // unread replies before a client counts as stalled
constexpr std::size_t kRecvChunk = 4096;
constexpr int kListenBacklog = 16;

// One thread serves every socket, so none of them may keep it busy: each poll() round
// takes a bounded amount from each. poll() is level-triggered, so the rest is picked
// up in the next round, after the other clients and the heartbeats had their turn.
constexpr int kMaxAcceptsPerRound = 16;
constexpr int kMaxReadsPerRound = 4;

std::string errno_text(const char* what) { return std::string(what) + ": " + std::strerror(errno); }

}  // namespace

struct Server::Slot {
    Slot(std::size_t queue_capacity, OverflowPolicy policy, std::size_t max_line)
        : inbuf(max_line), queue(queue_capacity, policy) {}

    // ---- io thread only ----
    int fd = -1;
    LineBuffer inbuf;
    std::string outbuf;
    Clock::time_point connected_at{}, last_rx{}, last_hb{};
    std::uint32_t hb_seq = 0;

    // ---- shared, guarded by mtx ----
    // Held only for a few field accesses, never across a send, so the producer
    // cannot be held up by a slow consumer.
    mutable std::mutex mtx;
    SlotState state = SlotState::Free;
    sockaddr_in peer{};
    sockaddr_in udp_dest{};
    std::uint64_t generation = 0;  // bumped on every release; stale queue items are skipped

    // Held by the sender for one whole frame. The sender rechecks the state before
    // every fragment, so once the io thread has moved the state away from STREAMING
    // and passed through this lock, no further datagram goes to this client.
    // Lock order: send_mtx -> mtx.
    std::mutex send_mtx;
    void wait_for_in_flight_frame() { std::lock_guard<std::mutex> lk(send_mtx); }

    // ---- thread-safe ----
    BoundedQueue<FrameItem> queue;
    std::atomic<std::uint64_t> frames_sent{0}, fragments_sent{0}, frames_dropped{0}, send_errors{0};
    std::atomic<std::size_t> pending_output{0};  // published copy of outbuf.size()
};

Server::Server(Config cfg) : cfg_(std::move(cfg)) {
    const auto target_us = std::chrono::duration_cast<std::chrono::microseconds>(cfg_.frame_interval).count();
    period_total_ = IntervalStats(target_us);
    period_window_ = IntervalStats(target_us);
}

Server::~Server() { stop(); }

// ---- lifecycle --------------------------------------------------------------

bool Server::start(std::string* error) {
    auto fail = [&](const std::string& msg) {
        if (error) *error = msg;
        listen_fd_.reset();
        return false;
    };
    if (running_) return true;
    if (auto msg = validate_config(cfg_); !msg.empty()) return fail(msg);
    set_log_level(cfg_.log_level);

    int pipefd[2];
    if (::pipe2(pipefd, O_NONBLOCK | O_CLOEXEC) != 0) return fail(errno_text("pipe2"));
    wake_rd_.reset(pipefd[0]);
    wake_wr_.reset(pipefd[1]);

    listen_fd_.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listen_fd_.valid()) return fail(errno_text("socket"));
    int one = 1;
    ::setsockopt(listen_fd_.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg_.tcp_port);
    if (::inet_pton(AF_INET, cfg_.bind_address.c_str(), &addr.sin_addr) != 1)
        return fail("bad bind_address " + cfg_.bind_address);
    if (::bind(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0)
        return fail(errno_text("bind"));
    if (::listen(listen_fd_.get(), kListenBacklog) != 0) return fail(errno_text("listen"));
    socklen_t len = sizeof addr;
    ::getsockname(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), &len);
    bound_port_ = ntohs(addr.sin_port);

    slots_.clear();
    udp_fds_.clear();
    for (std::size_t i = 0; i < cfg_.max_clients; ++i) {
        slots_.push_back(std::make_unique<Slot>(cfg_.queue_capacity, cfg_.queue_overflow, cfg_.max_line_bytes));
        UniqueFd udp(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!udp.valid()) return fail(errno_text("udp socket"));
        udp_fds_.push_back(std::move(udp));
    }
    fragments_per_frame_ = fragment_count_for(
        kFrameHeaderWireSize + cfg_.tracks_per_frame * kTrackWireSize, cfg_.max_datagram_bytes);

    stop_ = false;
    running_ = true;
    io_thread_ = std::thread(&Server::io_loop, this);
    producer_thread_ = std::thread(&Server::producer_loop, this);
    for (std::size_t i = 0; i < slots_.size(); ++i) sender_threads_.emplace_back(&Server::sender_loop, this, i);

    log_msg(LogLevel::Info, "listening on ", cfg_.bind_address, ":", bound_port_, " (max_clients=",
            cfg_.max_clients, ", frame=", cfg_.frame_interval.count(), "ms x ", fragments_per_frame_,
            " fragments, hb=", cfg_.heartbeat_interval.count(), "ms, zombie_timeout=",
            cfg_.zombie_timeout.count(), "ms, queue=", cfg_.queue_capacity, ")");
    return true;
}

void Server::stop() {
    if (!running_.exchange(false)) return;
    {
        std::lock_guard<std::mutex> lk(producer_mtx_);
        stop_ = true;
    }
    producer_cv_.notify_all();
    wake();

    // Order matters: io first (it sends BYE and releases every slot), then the
    // producer, then close the queues so the senders drain and exit.
    if (io_thread_.joinable()) io_thread_.join();
    if (producer_thread_.joinable()) producer_thread_.join();
    for (auto& s : slots_) s->queue.close();
    for (auto& t : sender_threads_)
        if (t.joinable()) t.join();
    sender_threads_.clear();
    listen_fd_.reset();

    const auto st = stats();
    log_msg(LogLevel::Info, "stopped: accepted=", st.accepted, " rejected_full=", st.rejected_full,
            " frames_produced=", st.frames_produced, " frames_dropped=", st.frames_dropped,
            " producer_overruns=", st.producer_overruns);
}

void Server::wake() {
    if (!wake_wr_.valid()) return;
    const char c = 1;
    [[maybe_unused]] auto n = ::write(wake_wr_.get(), &c, 1);
}

// ---- io thread --------------------------------------------------------------

void Server::io_loop() {
    std::vector<pollfd> pfds;
    std::vector<int> owner;  // -2 wake pipe, -1 listen socket, >= 0 slot index
    next_stats_report_ = Clock::now() + cfg_.stats_interval;

    while (!stop_.load()) {
        pfds.clear();
        owner.clear();
        pfds.push_back({wake_rd_.get(), POLLIN, 0});
        owner.push_back(-2);
        pfds.push_back({listen_fd_.get(), POLLIN, 0});
        owner.push_back(-1);
        for (std::size_t i = 0; i < slots_.size(); ++i) {
            const Slot& s = *slots_[i];
            if (s.fd < 0) continue;
            const short events = static_cast<short>(POLLIN | (s.outbuf.empty() ? 0 : POLLOUT));
            pfds.push_back({s.fd, events, 0});
            owner.push_back(static_cast<int>(i));
        }

        const int rc = ::poll(pfds.data(), pfds.size(), poll_timeout_ms(Clock::now()));
        if (rc < 0) {
            if (errno == EINTR) continue;
            log_msg(LogLevel::Error, errno_text("poll"));
            break;
        }
        const auto now = Clock::now();

        for (std::size_t k = 0; k < pfds.size() && rc > 0; ++k) {
            const short rev = pfds[k].revents;
            if (rev == 0) continue;
            if (owner[k] == -2) {
                char buf[64];
                while (::read(wake_rd_.get(), buf, sizeof buf) > 0) {
                }
                continue;
            }
            if (owner[k] == -1) {
                handle_accept(now);
                continue;
            }
            const auto i = static_cast<std::size_t>(owner[k]);
            Slot& s = *slots_[i];
            if (s.fd != pfds[k].fd) continue;  // closed earlier in this round
            if (rev & POLLNVAL) {
                close_slot(i, CloseReason::Reset);
                continue;
            }
            if (rev & (POLLIN | POLLHUP | POLLERR)) handle_readable(i, now);  // recv() reports EOF/errors
            if (s.fd >= 0 && (rev & POLLOUT) && !flush(i)) close_slot(i, CloseReason::Reset);
        }
        housekeeping(Clock::now());
    }

    for (std::size_t i = 0; i < slots_.size(); ++i)
        if (slots_[i]->fd >= 0) close_slot(i, CloseReason::Shutdown, "BYE SHUTDOWN");
}

int Server::poll_timeout_ms(Clock::time_point now) const {
    std::optional<Clock::time_point> next;
    if (cfg_.stats_interval.count() > 0) next = next_stats_report_;
    for (const auto& sp : slots_) {
        if (sp->fd < 0) continue;
        const auto d = next_liveness_deadline(sp->last_rx, sp->last_hb, cfg_.heartbeat_interval,
                                              cfg_.zombie_timeout);
        if (!next || d < *next) next = d;
    }
    if (!next) return -1;  // nothing scheduled: sleep until a socket or the wake pipe fires
    if (*next <= now) return 0;
    const auto ms = std::chrono::ceil<Millis>(*next - now).count();
    return static_cast<int>(std::min<long long>(ms, 60'000));
}

void Server::handle_accept(Clock::time_point now) {
    for (int round = 0; round < kMaxAcceptsPerRound; ++round) {
        sockaddr_in peer{};
        socklen_t len = sizeof peer;
        const int fd = ::accept4(listen_fd_.get(), reinterpret_cast<sockaddr*>(&peer), &len,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) log_msg(LogLevel::Warn, errno_text("accept"));
            return;
        }

        std::optional<std::size_t> free_index;
        for (std::size_t i = 0; i < slots_.size(); ++i) {
            if (slots_[i]->fd < 0) {
                free_index = i;
                break;
            }
        }
        if (!free_index) {
            ++rejected_full_;  // counted before the peer can observe the close
            static constexpr char kFull[] = "ERR FULL\n";
            [[maybe_unused]] auto n = ::send(fd, kFull, sizeof kFull - 1, MSG_NOSIGNAL | MSG_DONTWAIT);
            ::shutdown(fd, SHUT_WR);
            ::close(fd);
            log_msg(LogLevel::Info, "rejected ", to_string(peer), ": all ", slots_.size(), " slots in use");
            continue;
        }

        const int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

        const std::size_t i = *free_index;
        Slot& s = *slots_[i];
        s.fd = fd;
        s.inbuf.clear();
        s.outbuf.clear();
        s.connected_at = s.last_rx = s.last_hb = now;
        s.hb_seq = 0;
        s.frames_sent = 0;
        s.fragments_sent = 0;
        s.frames_dropped = 0;
        s.send_errors = 0;
        {
            std::lock_guard<std::mutex> lk(s.mtx);
            s.peer = peer;
            s.udp_dest = sockaddr_in{};
            s.state = SlotState::Connected;
        }
        ++active_;
        ++accepted_;
        log_msg(LogLevel::Info, "slot ", i, " <- ", to_string(peer), " (", active_.load(), "/", slots_.size(),
                " in use)");
        queue_line(i, "WELCOME RADARSIM/1.0 slot=" + std::to_string(i) + " max=" +
                          std::to_string(slots_.size()) + " hb_ms=" +
                          std::to_string(cfg_.heartbeat_interval.count()) + " timeout_ms=" +
                          std::to_string(cfg_.zombie_timeout.count()));
    }
}

void Server::handle_readable(std::size_t i, Clock::time_point now) {
    Slot& s = *slots_[i];
    char buf[kRecvChunk];
    for (int round = 0; round < kMaxReadsPerRound; ++round) {
        const ssize_t n = ::recv(s.fd, buf, sizeof buf, 0);
        if (n > 0) {
            s.last_rx = now;  // any byte proves the peer is alive
            s.inbuf.append(buf, static_cast<std::size_t>(n));
            while (auto line = s.inbuf.next_line()) {  // commands before a bad line are valid
                handle_line(i, *line);
                if (s.fd < 0) return;  // QUIT or a failed write closed the slot
            }
            if (s.inbuf.overflowed()) {
                queue_line(i, "ERR LINE_TOO_LONG");
                close_slot(i, CloseReason::Protocol);
                return;
            }
            continue;
        }
        if (n == 0) {
            close_slot(i, CloseReason::PeerClosed);
            return;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        log_msg(LogLevel::Info, "slot ", i, " ", errno_text("recv"));
        close_slot(i, CloseReason::Reset);
        return;
    }
}

void Server::handle_line(std::size_t i, const std::string& line) {
    Slot& s = *slots_[i];
    const Command cmd = parse_command(line);
    log_msg(LogLevel::Debug, "slot ", i, " >> ", line);

    switch (cmd.type) {
        case CommandType::Empty:
        case CommandType::Pong:
            return;  // liveness already updated by handle_readable
        case CommandType::Register: {
            sockaddr_in dest{};
            {
                std::lock_guard<std::mutex> lk(s.mtx);
                // Datagrams only go back to the address the TCP connection came from:
                // a client cannot aim the stream at a third party.
                dest = s.peer;
                dest.sin_port = htons(cmd.udp_port);
                s.udp_dest = dest;
                if (s.state == SlotState::Connected) s.state = SlotState::Registered;
            }
            queue_line(i, "OK REGISTERED udp=" + to_string(dest));
            return;
        }
        case CommandType::Start: {
            bool ok = false;
            {
                std::lock_guard<std::mutex> lk(s.mtx);
                if (s.state == SlotState::Registered || s.state == SlotState::Streaming) {
                    s.state = SlotState::Streaming;
                    ok = true;
                }
            }
            queue_line(i, ok ? "OK STREAMING" : "ERR NOT_REGISTERED");
            return;
        }
        case CommandType::Stop: {
            bool ok = false;
            {
                std::lock_guard<std::mutex> lk(s.mtx);
                if (s.state == SlotState::Registered || s.state == SlotState::Streaming) {
                    s.state = SlotState::Registered;
                    ok = true;
                }
            }
            s.wait_for_in_flight_frame();  // nothing is sent after the client reads OK STOPPED
            queue_line(i, ok ? "OK STOPPED" : "ERR NOT_REGISTERED");
            return;
        }
        case CommandType::Ping:
            queue_line(i, cmd.arg.empty() ? "PONG" : "PONG " + cmd.arg);
            return;
        case CommandType::Status:
            queue_line(i, status_line(i));
            return;
        case CommandType::Quit:
            close_slot(i, CloseReason::Quit, "BYE");
            return;
        case CommandType::Unknown:
            queue_line(i, "ERR UNKNOWN_COMMAND " + cmd.arg);
            return;
        case CommandType::Invalid:
            queue_line(i, "ERR BAD_ARGUMENT " + cmd.error);
            return;
    }
}

void Server::housekeeping(Clock::time_point now) {
    if (cfg_.stats_interval.count() > 0 && now >= next_stats_report_) {
        next_stats_report_ = now + cfg_.stats_interval;
        log_stats_report();
    }
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        Slot& s = *slots_[i];
        if (s.fd < 0) continue;
        if (judge_liveness(s.last_rx, now, cfg_.zombie_timeout) == Liveness::Zombie) {
            const auto silent = std::chrono::duration_cast<Millis>(now - s.last_rx).count();
            log_msg(LogLevel::Warn, "slot ", i, " zombie: nothing received for ", silent, "ms");
            close_slot(i, CloseReason::Timeout, "BYE TIMEOUT");
            continue;
        }
        if (heartbeat_due(s.last_hb, now, cfg_.heartbeat_interval)) {
            s.last_hb = now;
            queue_line(i, "HB " + std::to_string(++s.hb_seq));
        }
    }
}

std::string Server::status_line(std::size_t i) const {
    const Slot& s = *slots_[i];
    SlotState state;
    sockaddr_in dest;
    {
        std::lock_guard<std::mutex> lk(s.mtx);
        state = s.state;
        dest = s.udp_dest;
    }
    return "OK STATUS slot=" + std::to_string(i) + " state=" + to_string(state) +
           " udp_port=" + std::to_string(ntohs(dest.sin_port)) +
           " frames_sent=" + std::to_string(s.frames_sent.load()) +
           " fragments_sent=" + std::to_string(s.fragments_sent.load()) +
           " dropped=" + std::to_string(s.frames_dropped.load()) +
           " send_errors=" + std::to_string(s.send_errors.load()) +
           " queue=" +std::to_string(s.queue.size()) + "/" + std::to_string(s.queue.capacity());
}

bool Server::queue_line(std::size_t i, const std::string& line) {
    Slot& s = *slots_[i];
    if (s.fd < 0) return false;
    s.outbuf += line;
    s.outbuf += '\n';
    if (s.outbuf.size() > kMaxOutbuf) {
        log_msg(LogLevel::Warn, "slot ", i, " zombie: client stopped reading (", s.outbuf.size(),
                " bytes pending)");
        close_slot(i, CloseReason::Stalled);
        return false;
    }
    if (!flush(i)) {
        close_slot(i, CloseReason::Reset);
        return false;
    }
    return true;
}

bool Server::flush(std::size_t i) {
    Slot& s = *slots_[i];
    while (!s.outbuf.empty()) {
        const ssize_t n = ::send(s.fd, s.outbuf.data(), s.outbuf.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0) {
            s.outbuf.erase(0, static_cast<std::size_t>(n));
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {  // POLLOUT will resume
            s.pending_output = s.outbuf.size();
            return true;
        }
        return false;
    }
    s.pending_output = 0;
    return true;
}

void Server::close_slot(std::size_t i, CloseReason reason, const char* bye) {
    Slot& s = *slots_[i];
    if (s.fd < 0) return;

    sockaddr_in peer;
    {
        std::lock_guard<std::mutex> lk(s.mtx);
        s.state = SlotState::Closing;
        peer = s.peer;
    }
    s.wait_for_in_flight_frame();
    const std::size_t discarded = s.queue.clear();
    ++closed_[static_cast<std::size_t>(reason)];  // counted before the peer can observe the close

    if (bye && reason != CloseReason::Stalled) {
        s.outbuf += bye;
        s.outbuf += '\n';
        flush(i);  // best effort
    }
    // Half-close first and drain whatever the client still sent, so close() sends FIN
    // rather than RST (an RST could destroy the BYE before the client reads it).
    ::shutdown(s.fd, SHUT_WR);
    char sink[512];
    for (int k = 0; k < 16 && ::recv(s.fd, sink, sizeof sink, MSG_DONTWAIT) > 0; ++k) {
    }
    ::close(s.fd);
    s.fd = -1;
    s.inbuf.clear();
    s.outbuf.clear();
    s.pending_output = 0;

    {
        std::lock_guard<std::mutex> lk(s.mtx);
        s.state = SlotState::Free;
        ++s.generation;
    }
    --active_;

    const auto alive_ms = std::chrono::duration_cast<Millis>(Clock::now() - s.connected_at).count();
    log_msg(LogLevel::Info, "slot ", i, " released (", to_string(reason), ") ", to_string(peer),
            " alive=", alive_ms, "ms frames_sent=", s.frames_sent.load(), " dropped=", s.frames_dropped.load(),
            " discarded_in_queue=", discarded, " (", active_.load(), "/", slots_.size(), " in use)");
}

// ---- producer ---------------------------------------------------------------

void Server::producer_loop() {
    FakeTrackGenerator gen(cfg_.tracks_per_frame);
    const double dt = std::chrono::duration<double>(cfg_.frame_interval).count();
    auto next = Clock::now();
    std::optional<Clock::time_point> last_tick;

    std::unique_lock<std::mutex> lk(producer_mtx_);
    while (!stop_.load()) {
        lk.unlock();
        const auto tick = Clock::now();
        if (last_tick) {
            const auto us = std::chrono::duration_cast<std::chrono::microseconds>(tick - *last_tick).count();
            std::lock_guard<std::mutex> plk(period_mtx_);
            period_total_.add(us);
            period_window_.add(us);
        }
        last_tick = tick;

        const auto ts = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
        auto frame = std::make_shared<const PreparedFrame>(
            prepare_frame(gen.next(static_cast<std::uint64_t>(ts), dt), cfg_.max_datagram_bytes));
        ++frames_produced_;

        for (auto& sp : slots_) {
            Slot& s = *sp;
            std::lock_guard<std::mutex> slk(s.mtx);
            if (s.state != SlotState::Streaming) continue;
            const PushResult r = s.queue.push({frame, s.generation});
            if (r == PushResult::DroppedOldest || r == PushResult::DroppedNewest) {
                ++s.frames_dropped;
                ++frames_dropped_;
            }
        }
        lk.lock();

        // Fixed-rate schedule. If we fell more than a whole period behind, resync
        // instead of bursting to catch up.
        next += cfg_.frame_interval;
        const auto now = Clock::now();
        if (now > next + cfg_.frame_interval) {
            ++producer_overruns_;
            next = now;
        }
        producer_cv_.wait_until(lk, next, [&] { return stop_.load(); });
    }
}

// ---- senders ----------------------------------------------------------------

void Server::sender_loop(std::size_t i) {
    Slot& s = *slots_[i];
    const int udp = udp_fds_[i].get();
    while (auto item = s.queue.pop()) {
        std::lock_guard<std::mutex> send_lk(s.send_mtx);
        bool complete = true;
        for (const auto& frag : item->frame->fragments) {
            sockaddr_in dest;
            {
                std::lock_guard<std::mutex> lk(s.mtx);
                if (s.state != SlotState::Streaming || s.generation != item->generation) {
                    complete = false;
                    break;
                }
                dest = s.udp_dest;
            }
            const ssize_t n = ::sendto(udp, frag.data(), frag.size(), 0,
                                       reinterpret_cast<const sockaddr*>(&dest), sizeof dest);
            if (n == static_cast<ssize_t>(frag.size()))
                ++s.fragments_sent;
            else
                ++s.send_errors;
        }
        if (complete)
            ++s.frames_sent;
        else
            ++stale_skipped_;
    }
}

// ---- introspection ----------------------------------------------------------

std::unique_lock<std::mutex> Server::hold_sender_for_test(std::size_t index) {
    return std::unique_lock<std::mutex>(slots_[index]->send_mtx);
}

std::vector<SlotSnapshot> Server::snapshot() const {
    std::vector<SlotSnapshot> out;
    out.reserve(slots_.size());
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        const Slot& s = *slots_[i];
        SlotSnapshot snap;
        snap.index = i;
        {
            std::lock_guard<std::mutex> lk(s.mtx);
            snap.state = s.state;
            if (s.state != SlotState::Free) snap.peer = to_string(s.peer);
            snap.udp_port = ntohs(s.udp_dest.sin_port);
        }
        snap.frames_sent = s.frames_sent.load();
        snap.fragments_sent = s.fragments_sent.load();
        snap.frames_dropped = s.frames_dropped.load();
        snap.send_errors = s.send_errors.load();
        snap.queue_depth = s.queue.size();
        snap.pending_output_bytes = s.pending_output.load();
        out.push_back(std::move(snap));
    }
    return out;
}

ServerStats Server::stats() const {
    ServerStats st;
    st.accepted = accepted_.load();
    st.rejected_full = rejected_full_.load();
    st.frames_produced = frames_produced_.load();
    st.frames_dropped = frames_dropped_.load();
    st.stale_frames_skipped = stale_skipped_.load();
    st.producer_overruns = producer_overruns_.load();
    for (std::size_t r = 0; r < kCloseReasonCount; ++r) st.closed[r] = closed_[r].load();
    std::lock_guard<std::mutex> lk(period_mtx_);
    st.frame_period = period_total_;
    return st;
}

void Server::log_stats_report() {
    IntervalStats window;
    {
        std::lock_guard<std::mutex> lk(period_mtx_);
        window = period_window_;
        period_window_.reset();
    }
    if (!log_enabled(LogLevel::Info)) return;
    for (const auto& line : format_stats_report(stats(), snapshot(), window)) log_msg(LogLevel::Info, line);
}

std::vector<std::string> format_stats_report(const ServerStats& stats, const std::vector<SlotSnapshot>& slots,
                                             const IntervalStats& window) {
    std::size_t in_use = 0;
    for (const auto& s : slots) in_use += s.state != SlotState::Free;

    char period[128];
    std::snprintf(period, sizeof period, "period_ms avg=%.2f min=%.2f max=%.2f jitter=%.2f", window.mean_us() / 1000.0,
                  static_cast<double>(window.min_us()) / 1000.0, static_cast<double>(window.max_us()) / 1000.0,
                  window.jitter_us() / 1000.0);

    std::vector<std::string> lines;
    lines.push_back("stats: clients=" + std::to_string(in_use) + "/" + std::to_string(slots.size()) +
                    " produced=" + std::to_string(stats.frames_produced) +
                    " dropped=" + std::to_string(stats.frames_dropped) +
                    " rejected_full=" + std::to_string(stats.rejected_full) +
                    " overruns=" + std::to_string(stats.producer_overruns) + " " + period);
    for (const auto& s : slots) {
        if (s.state == SlotState::Free) continue;
        lines.push_back("stats: slot " + std::to_string(s.index) + " " + to_string(s.state) + " peer=" + s.peer +
                        " udp_port=" + std::to_string(s.udp_port) + " frames_sent=" + std::to_string(s.frames_sent) +
                        " fragments_sent=" + std::to_string(s.fragments_sent) +
                        " dropped=" + std::to_string(s.frames_dropped) + " send_errors=" + std::to_string(s.send_errors) +
                        " queue=" + std::to_string(s.queue_depth) +
                        " pending_output=" + std::to_string(s.pending_output_bytes));
    }
    return lines;
}

}  // namespace radar
