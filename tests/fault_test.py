#!/usr/bin/env python3
"""Fault injection against the real radar_server / radar_client binaries.

    python3 tests/fault_test.py [build_dir]

Runs inside its own network namespace (unshare -Urn): no sudo is needed, iptables and
netem rules never touch the machine's real network, and port 8088 is always free.
Needs iproute2 (tc) and iptables. Exit code 0 when every check passes.
"""
import os
import re
import resource
import signal
import subprocess
import sys
import tempfile
import time

if "RADAR_IN_NETNS" not in os.environ:
    os.environ["RADAR_IN_NETNS"] = "1"
    os.execvp("unshare", ["unshare", "-Urn", sys.executable, os.path.abspath(__file__)] + sys.argv[1:])

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import smoke_test as st  # noqa: E402  (shares its Client, check() and argv handling)

check, wait_for = st.check, st.wait_for
SERVER_BIN, CLIENT_BIN = st.SERVER_BIN, st.CLIENT_BIN
PORT = 18200
HB_MS, TIMEOUT_MS = 200, 800
FAST_CONF = f"tcp_port = {PORT}\nheartbeat_interval_ms = {HB_MS}\nzombie_timeout_ms = {TIMEOUT_MS}\n"
ZOMBIE_VERDICT = "zombie: nothing received"


def sh(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class Server:
    def __init__(self, conf=FAST_CONF, args=None, nofile=None):
        self.log = tempfile.NamedTemporaryFile("w+", suffix=".log")
        cmd = [SERVER_BIN] + (args or [])
        if conf is not None:
            self.conf = tempfile.NamedTemporaryFile("w", suffix=".conf")
            self.conf.write(conf)
            self.conf.flush()
            cmd += ["-c", self.conf.name]

        def limit():
            if nofile:
                resource.setrlimit(resource.RLIMIT_NOFILE, (nofile, nofile))

        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=self.log, text=True, preexec_fn=limit)
        line = self.proc.stdout.readline()
        m = re.search(r"port (\d+)", line)
        self.port = int(m.group(1)) if m else None

    def log_text(self):
        self.log.seek(0)
        return self.log.read()

    def cpu_seconds(self):
        f = open(f"/proc/{self.proc.pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGCONT)
            self.proc.terminate()
        self.proc.wait()


class ClientProc:
    """radar_client with its output kept in a file, so progress can be watched."""

    def __init__(self, port=PORT, args=None, fast=True):
        self.out = tempfile.NamedTemporaryFile("w+", suffix=".out")
        cmd = [CLIENT_BIN, "--port", str(port)]
        if fast:
            cmd += ["--reconnect-min-ms", "100", "--reconnect-max-ms", "400"]
        self.proc = subprocess.Popen(cmd + (args or []), stdout=self.out, stderr=subprocess.STDOUT)

    def text(self):
        self.out.seek(0)
        return self.out.read()

    def count(self, what):
        return self.text().count(what)

    def stop(self):
        """Ctrl+C, and the numbers from its SUMMARY line."""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        return self.summary()

    def summary(self):
        m = re.search(r"SUMMARY (.*)", self.text())
        return {k: float(v) for k, v in re.findall(r"(\w+)=([\d.]+)", m.group(1))} if m else {}


def test_config_file():
    print("\n1. settings really come from the config file")
    s = Server(conf=None, args=["-c", "config/server.conf"])
    check("config/server.conf listens on 8088", s.port == 8088, f"port={s.port}")
    c = st.Client(8088)
    w = c.reply() or ""
    check("welcome shows max=3", "max=3 " in w and "hb_ms=1000" in w and "timeout_ms=3500" in w, w)
    c.close()
    s.stop()

    s = Server(conf=None)
    check("no config file: default 8088", s.port == 8088, f"port={s.port}")
    s.stop()

    s = Server(conf="tcp_port = 18123\nmax_clients = 2\n")
    check("tcp_port from the file", s.port == 18123, f"port={s.port}")
    clients = [st.Client(18123) for _ in range(2)]
    check("max_clients from the file", all("max=2 " in (c.reply() or "") for c in clients))
    third = st.Client(18123)
    check("3rd client gets ERR FULL with max_clients = 2", third.reply() == "ERR FULL")
    third.close()
    for c in clients:
        c.close()

    busy = Server(conf="tcp_port = 18123\n")
    busy.proc.wait(timeout=5)
    check("port already in use: exits with code 1", busy.proc.returncode == 1, busy.log_text().strip()[:70])
    s.stop()

    for name, conf in (("unknown key", "tcp_prot = 1\n"), ("bad value", "max_clients = 0\n")):
        bad = Server(conf=conf)
        bad.proc.wait(timeout=5)
        check(f"{name}: exits with code 1", bad.proc.returncode == 1, bad.log_text().strip()[:70])
    missing = subprocess.run([SERVER_BIN, "-c", "/nonexistent.conf"], capture_output=True, text=True)
    check("missing file: exits with code 1", missing.returncode == 1, missing.stderr.strip()[:70])


def test_fd_exhaustion():
    print("\n2. out of file descriptors")
    # Enough for the server itself and one client; connecting more runs accept() out of fds.
    s = Server(conf=f"tcp_port = {PORT}\nheartbeat_interval_ms = {HB_MS}\nzombie_timeout_ms = 5000\nlog_level = warn\n",
               nofile=11)
    held = []
    refused = None
    for _ in range(3):
        c = st.Client(PORT)
        first = c.reply(timeout=0.5)
        if first and first.startswith("WELCOME"):
            held.append(c)
        else:
            refused = (c, first)
            break
    check("the limit is reached before the slots are", refused is not None, f"{len(held)} clients got in")
    if refused is None:
        s.stop()
        return

    cpu0, t0, size0 = s.cpu_seconds(), time.monotonic(), len(s.log_text())
    time.sleep(2)
    cpu = 100 * (s.cpu_seconds() - cpu0) / (time.monotonic() - t0)
    grown = len(s.log_text()) - size0
    check("server does not spin while out of fds", cpu < 20, f"CPU {cpu:.0f}%")
    check("log is not flooded", grown < 2000, f"{grown} bytes in 2 s")

    c, first = refused
    check("the extra client is told, not left hanging", first == "ERR BUSY", f"first line: {first}")
    held[0].send("PING alive")
    check("connected clients keep working", held[0].reply() == "PONG alive")
    c.close()

    held[0].send("QUIT")
    held[0].reply()
    held[0].close()
    for h in held[1:]:
        h.close()

    def can_join():
        n = st.Client(PORT)
        line = n.reply(timeout=0.5)
        n.close()
        return bool(line and line.startswith("WELCOME"))

    check("new clients get in once fds are free", wait_for(can_join, 3))
    s.stop()


def test_server_failures():
    print("\n3. server killed and frozen")
    s = Server()
    client = ClientProc()
    wait_for(lambda: client.count("READY") == 1, 3)
    s.proc.kill()
    s.proc.wait()
    time.sleep(0.5)
    s = Server()
    check("server kill -9: client comes back on restart", wait_for(lambda: client.count("READY") == 2, 5))
    time.sleep(0.5)
    r = client.stop()
    check("  frame numbers restarting is not counted as loss", r.get("lost") == 0 and r.get("sessions") == 2, str(r))
    s.stop()

    s = Server(conf=FAST_CONF + "stats_interval_ms = 0\n")
    client = ClientProc()
    plain = st.Client(PORT)  # answers heartbeats, never reconnects
    plain.reply()
    wait_for(lambda: client.count("READY") == 1, 3)
    time.sleep(0.5)
    s.proc.send_signal(signal.SIGSTOP)
    silent = wait_for(lambda: "server silent" in client.text(), 3)
    check("server frozen: client notices the silence", silent)
    time.sleep(1.5)
    s.proc.send_signal(signal.SIGCONT)
    t0 = time.monotonic()
    back = wait_for(lambda: client.count("READY") >= 2, 12)
    check("server resumes: client streams again", back, f"after {time.monotonic() - t0:.1f} s")
    time.sleep(0.3)
    check("a client that did nothing wrong is not reaped after the freeze", not plain.closed)
    log = s.log_text()
    check("  no false zombie verdicts in the log", ZOMBIE_VERDICT not in log, str(log.count(ZOMBIE_VERDICT)) + " verdicts")
    check("  the stall itself is logged", "server did not run for" in log)
    plain.close()
    client.stop()
    s.stop()


def cut_network():
    sh("iptables", "-A", "INPUT", "-i", "lo", "-j", "DROP")


def restore_network():
    sh("iptables", "-F")


def test_cable_cut():
    print("\n4. cable cut: packets vanish, nobody gets a FIN")
    s = Server()
    client = ClientProc()
    wait_for(lambda: client.count("READY") == 1, 3)
    time.sleep(0.3)

    cut_network()
    t0 = time.monotonic()
    reaped = wait_for(lambda: ZOMBIE_VERDICT in s.log_text(), 3)
    took = (time.monotonic() - t0) * 1000
    check("server reaps the client by timeout", reaped, f"{took:.0f} ms (timeout {TIMEOUT_MS} ms)")
    check("  close to the timeout", TIMEOUT_MS - HB_MS <= took <= TIMEOUT_MS + 500)
    check("client notices the silence", wait_for(lambda: "server silent" in client.text(), 3))

    time.sleep(8)  # long enough for the kernel's SYN retries to back off
    restore_network()
    t0 = time.monotonic()
    back = wait_for(lambda: client.count("READY") == 2, 20)
    took = time.monotonic() - t0
    check("cable back: client streams again", back, f"{took:.1f} s after restoring")
    check("  without waiting for the kernel's SYN retries", took < 5)
    client.stop()

    cut_network()
    t0 = time.monotonic()
    timed = ClientProc(args=["--duration-ms", "1000"])
    try:
        timed.proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        timed.proc.kill()
    took = time.monotonic() - t0
    check("--duration-ms holds while connect() hangs", took < 3, f"exited after {took:.1f} s")
    restore_network()

    cut_network()
    hanging = ClientProc()
    time.sleep(0.5)
    t0 = time.monotonic()
    hanging.proc.send_signal(signal.SIGINT)
    try:
        hanging.proc.wait(timeout=5)
        took = time.monotonic() - t0
    except subprocess.TimeoutExpired:
        hanging.proc.kill()
        took = 5
    check("Ctrl+C works while connect() hangs", took < 1, f"{took:.2f} s")
    restore_network()
    s.stop()


def with_netem(*rule):
    sh("tc", "qdisc", "add", "dev", "lo", "root", "netem", *rule)


def stream_under(*rule, seconds=5):
    s = Server(conf=f"tcp_port = {PORT}\nlog_level = warn\n")  # default heartbeats and timeout
    client = ClientProc(fast=False)
    wait_for(lambda: client.count("READY") == 1, 3)
    with_netem(*rule)  # only now, so a retransmitted handshake cannot shorten the measurement
    time.sleep(seconds)
    r = client.stop()  # before removing netem: that throws away the packets it still holds
    sh("tc", "qdisc", "del", "dev", "lo", "root")
    s.stop()
    print(f"     netem {' '.join(rule)}: {r}")
    return r


def test_impaired_network():
    print("\n5. impaired network (netem)")
    expected = 5000 / 35

    r = stream_under("loss", "5%")
    survive = 0.95 ** 4  # a frame needs all 4 of its fragments
    check("5% loss: invalid data never accepted", r.get("invalid") == 0)
    check("5% loss: one connection throughout", r.get("sessions") == 1, f"sessions={r.get('sessions')}")
    pct = r.get("loss_pct", -1)
    check("5% loss: frame loss near the expected 19%", 8 <= pct <= 30, f"{pct}%, expected {100 * (1 - survive):.0f}%")
    check("5% loss: received + lost covers the stream", abs(r.get("frames", 0) + r.get("lost", 0) - expected) < 12,
          f"{r.get('frames', 0) + r.get('lost', 0):.0f} of about {expected:.0f}")
    check("5% loss: lost frames show up as incomplete", abs(r.get("incomplete", -99) - r.get("lost", 0)) <= 3,
          f"incomplete={r.get('incomplete')} lost={r.get('lost')}")

    r = stream_under("delay", "50ms")
    # The average includes the few frames from before netem was added.
    check("50 ms delay: latency measured correctly", 45 <= r.get("latency_ms_avg", 0) <= 60 and r.get("latency_ms_max", 0) >= 49,
          f"avg {r.get('latency_ms_avg')} ms, max {r.get('latency_ms_max')} ms")
    check("50 ms delay: nothing lost", r.get("lost") == 0, f"lost={r.get('lost')}")

    r = stream_under("delay", "20ms", "10ms", "reorder", "25%", "50%")
    check("reordering: frames still reassemble", r.get("frames", 0) >= expected - 12 and r.get("invalid") == 0,
          f"frames={r.get('frames')}")
    check("reordering: late frames are not called lost", r.get("lost", 99) <= 2, f"lost={r.get('lost')}")

    r = stream_under("duplicate", "20%")
    check("20% duplicates: each frame counted once", abs(r.get("frames", 0) - expected) < 12, f"frames={r.get('frames')}")
    check("20% duplicates: nothing reported lost", r.get("lost", 99) == 0, f"lost={r.get('lost')}")
    check("20% duplicates: no phantom incomplete frames", r.get("incomplete", 99) <= 3, f"incomplete={r.get('incomplete')}")

    r = stream_under("loss", "30%", seconds=8)
    check("30% loss: invalid data never accepted", r.get("invalid") == 0)
    print(f"     30% loss: sessions={r.get('sessions')} (for information)")


def main():
    sh("ip", "link", "set", "lo", "up")
    for scenario in (test_config_file, test_fd_exhaustion, test_server_failures, test_cable_cut, test_impaired_network):
        try:
            scenario()
        except Exception as e:  # a crashed scenario is a failure, and the next one still runs
            check(f"{scenario.__name__} crashed: {type(e).__name__}: {e}", False)
        finally:
            subprocess.run(["iptables", "-F"], stderr=subprocess.DEVNULL)
            subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], stderr=subprocess.DEVNULL)
            subprocess.run(["pkill", "-x", "radar_client"], stderr=subprocess.DEVNULL)
            subprocess.run(["pkill", "-x", "radar_server"], stderr=subprocess.DEVNULL)
            time.sleep(0.3)
    passed = sum(st.results)
    print(f"\n{passed}/{len(st.results)} checks passed")
    return 0 if passed == len(st.results) else 1


if __name__ == "__main__":
    sys.exit(main())
