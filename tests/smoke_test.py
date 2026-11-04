#!/usr/bin/env python3
"""Black-box smoke test: drives the real radar_server / radar_client binaries
over real sockets, using only the Python standard library.

    python3 tests/smoke_test.py [build_dir]

Exit code 0 when every check passes.
"""
import os
import queue
import re
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

BUILD = sys.argv[1] if len(sys.argv) > 1 else "build"
SERVER_BIN = os.path.join(BUILD, "radar_server")
CLIENT_BIN = os.path.join(BUILD, "radar_client")

HB_MS, TIMEOUT_MS = 200, 800
FRAG_MAGIC = 0x31524452  # "RDR1"
FRAG_HEADER = struct.Struct("<IIHHII")  # magic, seq, index, count, total, offset

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


class Client:
    """Line-based TCP client with an optional UDP receiver.

    A background reader thread answers every "HB <n>" with "PONG <n>" (unless
    auto_pong is False), so a client stays healthy no matter what the test is
    waiting on. Every other line goes to a queue for reply()."""

    _EOF = object()

    def __init__(self, port, auto_pong=True):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=2)
        self.sock.settimeout(None)
        self.auto_pong = auto_pong
        self.udp = None
        self.heartbeats = 0
        self.closed = False
        self.lines = queue.Queue()
        self.send_lock = threading.Lock()
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        buf = b""
        try:
            while True:
                data = self.sock.recv(4096)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    raw, buf = buf.split(b"\n", 1)
                    line = raw.decode().rstrip("\r")
                    if line.startswith("HB "):
                        self.heartbeats += 1
                        if self.auto_pong:
                            self.send("PONG " + line[3:])
                    else:
                        self.lines.put(line)
        except OSError:
            pass
        self.closed = True
        self.lines.put(self._EOF)

    def send(self, line):
        with self.send_lock:
            self.sock.sendall(line.encode() + b"\n")

    def reply(self, timeout=2.0):
        """Next non-heartbeat line; None on timeout or once the connection is closed."""
        try:
            line = self.lines.get(timeout=timeout)
        except queue.Empty:
            return None
        if line is self._EOF:
            self.lines.put(self._EOF)  # keep reporting EOF
            return None
        return line

    def is_closed(self, timeout=2.0):
        return wait_for(lambda: self.closed, timeout)

    def open_udp(self):
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        self.udp.bind(("127.0.0.1", 0))
        self.udp.setblocking(False)
        return self.udp.getsockname()[1]

    def drain_udp(self):
        """Returns (fragments, complete_frames) received so far."""
        frags = 0
        seen, expected = {}, {}  # frame seq -> fragment indexes received / fragment count
        while True:
            try:
                data = self.udp.recv(65536)
            except BlockingIOError:
                break
            magic, seq, idx, count, _, _ = FRAG_HEADER.unpack_from(data)
            if magic != FRAG_MAGIC:
                continue
            frags += 1
            seen.setdefault(seq, set()).add(idx)
            expected[seq] = count
        complete = sum(1 for seq, idxs in seen.items() if len(idxs) == expected[seq])
        return frags, complete

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        if self.udp:
            self.udp.close()


def start_server():
    cfg = tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False)
    cfg.write(f"tcp_port = 0\nheartbeat_interval_ms = {HB_MS}\n"
              f"zombie_timeout_ms = {TIMEOUT_MS}\nlog_level = warn\n")
    cfg.close()
    proc = subprocess.Popen([SERVER_BIN, "-c", cfg.name], stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
    m = re.search(r"port (\d+)", proc.stdout.readline())
    os.unlink(cfg.name)
    if not m:
        proc.kill()
        sys.exit("server did not start")
    return proc, int(m.group(1))


def connect_ok(port):
    c = Client(port)
    welcome = c.reply()
    assert welcome and welcome.startswith("WELCOME"), welcome
    return c


def wait_for(pred, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return pred()


def can_connect(port):
    """True if a new client gets a slot (it then quits politely)."""
    c = Client(port)
    line = c.reply()
    if line and line.startswith("WELCOME"):
        c.send("QUIT")
        c.reply()
    c.close()
    return bool(line and line.startswith("WELCOME"))


def main():
    for b in (SERVER_BIN, CLIENT_BIN):
        if not os.access(b, os.X_OK):
            sys.exit(f"missing binary {b} - build first")

    server, port = start_server()
    print(f"radar_server on port {port} (hb={HB_MS}ms, zombie_timeout={TIMEOUT_MS}ms)")
    try:
        print("\n1. welcome + basic commands")
        c = connect_ok(port)
        c.send("PING hello")
        check("PING -> PONG", c.reply() == "PONG hello")
        c.send("START")
        check("START before REGISTER rejected", c.reply() == "ERR NOT_REGISTERED")
        c.send("BOGUS")
        check("unknown command reported", c.reply() == "ERR UNKNOWN_COMMAND BOGUS")
        c.send("QUIT")
        check("QUIT -> BYE", c.reply() == "BYE")
        check("connection closed after QUIT", c.is_closed())
        c.close()

        print("\n2. 4 concurrent connections -> 4th rejected")
        three = [connect_ok(port) for _ in range(3)]
        fourth = Client(port)
        check("4th gets ERR FULL", fourth.reply() == "ERR FULL")
        check("4th is disconnected", fourth.is_closed())
        fourth.close()

        print("\n3. 3 clients streaming")
        for cl in three:
            cl.send(f"REGISTER {cl.open_udp()}")
            assert cl.reply().startswith("OK REGISTERED")
            cl.send("START")
            assert cl.reply() == "OK STREAMING"
        time.sleep(1.5)  # reader threads keep answering heartbeats
        for cl in three:
            cl.send("STOP")
            assert cl.reply() == "OK STOPPED"
        time.sleep(0.15)
        for i, cl in enumerate(three):
            frags, frames = cl.drain_udp()
            cl.send("STATUS")
            sent = int(re.search(r"fragments_sent=(\d+)", cl.reply()).group(1))
            check(f"client {i}: received {frags} fragments / {frames} frames, server sent {sent}",
                  frags == sent and frames >= 30 and frags == frames * 4)
        for cl in three:
            cl.send("QUIT")
            cl.reply()
            cl.close()

        print("\n4. kill -9 a client -> slot reclaimed")
        others = [connect_ok(port) for _ in range(2)]
        victim = subprocess.Popen([CLIENT_BIN, "--port", str(port)], stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL, text=True)
        ready = ""
        for _ in range(10):  # radar_client prints "<< WELCOME ..." etc. before READY
            ready = victim.stdout.readline()
            if not ready or ready.startswith("READY"):
                break
        check("radar_client process streaming", ready.startswith("READY"), ready.strip())
        probe = Client(port)
        check("server full before kill", probe.reply() == "ERR FULL")
        probe.close()
        t0 = time.monotonic()
        victim.send_signal(signal.SIGKILL)
        victim.wait()
        ok = wait_for(lambda: can_connect(port), 2.0)
        check("slot free again after SIGKILL", ok, f"{(time.monotonic() - t0) * 1000:.0f} ms")

        print("\n5. zombie: client that never answers heartbeats")
        zombie = Client(port, auto_pong=False)
        t0 = time.monotonic()
        assert zombie.reply().startswith("WELCOME")
        line = zombie.reply(timeout=3.0)  # HB lines arrive but are never answered
        elapsed = (time.monotonic() - t0) * 1000
        check("zombie gets BYE TIMEOUT", line == "BYE TIMEOUT", f"after {elapsed:.0f} ms")
        check(f"detected near zombie_timeout ({TIMEOUT_MS} ms)",
              TIMEOUT_MS - HB_MS <= elapsed <= TIMEOUT_MS + 400)
        check("zombie disconnected", zombie.is_closed())
        zombie.close()
        for o in others:  # these answered their heartbeats meanwhile
            o.send("PING alive")
            check("healthy client survived", o.reply() == "PONG alive")

        print("\n6. forged UDP datagrams -> radar_client keeps its memory")
        target = subprocess.Popen([CLIENT_BIN, "--port", str(port)], stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL, text=True)
        ready = ""
        for _ in range(10):
            ready = target.stdout.readline()
            if not ready or ready.startswith("READY"):
                break
        udp_port = int(re.search(r"udp_port=(\d+)", ready).group(1))

        def rss_kb():
            with open(f"/proc/{target.pid}/status") as f:
                return int(re.search(r"VmRSS:\s+(\d+)", f.read()).group(1))

        before = rss_kb()
        forger = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for i in range(40):  # each claims to start a 64 MB frame
            forger.sendto(FRAG_HEADER.pack(FRAG_MAGIC, 1000000 + i, 0, 2, 64 << 20, 0) + b"\0" * 100,
                          ("127.0.0.1", udp_port))
        forger.close()
        time.sleep(0.3)
        growth_mb = (rss_kb() - before) / 1024
        check("memory did not grow", growth_mb < 50, f"{growth_mb:.1f} MB")
        check("radar_client still running", target.poll() is None)
        target.send_signal(signal.SIGINT)
        out = target.communicate(timeout=5)[0]
        summary = re.search(r"SUMMARY .*frames=(\d+).* invalid=(\d+)", out)
        check("forged datagrams counted as invalid, real frames still arrive",
              bool(summary) and int(summary.group(2)) >= 40 and int(summary.group(1)) > 0,
              summary.group(0) if summary else "no SUMMARY")

        print("\n7. SIGINT -> graceful shutdown")
        server.send_signal(signal.SIGINT)
        for o in others:
            check("client gets BYE SHUTDOWN", o.reply() == "BYE SHUTDOWN")
            o.close()
        try:
            code = server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            code = None
        check("server exits with code 0", code == 0, f"code={code}")
    except Exception as e:  # a crashed scenario is a failure, not a silent pass
        check(f"unexpected error: {type(e).__name__}: {e}", False)
    finally:
        if server.poll() is None:
            server.kill()

    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
