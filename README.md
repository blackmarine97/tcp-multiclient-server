# TCP Multi-Client Server

[![CI](https://github.com/blackmarine97/tcp-multiclient-server/actions/workflows/ci.yml/badge.svg)](https://github.com/blackmarine97/tcp-multiclient-server/actions/workflows/ci.yml)

C++17 · POSIX sockets · Producer–Consumer · heartbeats and zombie handling

A multi-client server that runs on a plain PC, without radar hardware. Up to 3 clients connect to a TCP
command port, and each one receives fake radar track frames over UDP, generated every 35 ms. The server
sends heartbeats, reaps dead (zombie) connections, and shuts down gracefully.

- Design document: [docs/DESIGN.md](docs/DESIGN.md) (in Korean): thread structure and queues, slot state
  diagram, zombie criteria and rationale, and why POSIX sockets were chosen
- Sockets: POSIX sockets + `poll()` (rationale in DESIGN.md §2)

## Requirements

- Linux (tested on Ubuntu 22.04; on Windows, build and run inside **WSL**)
- g++ ≥ 9 (C++17), CMake ≥ 3.16
- GoogleTest: if it is not installed, CMake downloads v1.14.0 automatically (needs internet access)

```bash
sudo apt install build-essential cmake
```

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

Outputs: `build/radar_server`, `build/radar_client`, `build/unit_tests`, `build/integration_tests`

## Run

### Server

```bash
./build/radar_server -c config/server.conf
```

- `-c <file>`: config file (defaults are used without one), `-p <port>`: override the port
- Ctrl+C (SIGINT) or SIGTERM: sends `BYE SHUTDOWN` to every client, then exits cleanly

Config file [config/server.conf](config/server.conf):

| Key | Default | Meaning |
|---|---|---|
| `tcp_port` | 8088 | Command port (0 = pick an ephemeral port) |
| `max_clients` | 3 | Maximum concurrent clients |
| `frame_interval_ms` | 35 | Frame generation period |
| `heartbeat_interval_ms` | 1000 | How often the server sends `HB` |
| `zombie_timeout_ms` | 3500 | A client that sends nothing for this long is a zombie |
| `queue_capacity` | 8 | Per-client frame queue size |
| `queue_overflow` | drop_oldest | When the queue is full: `drop_oldest` / `drop_newest` |
| `tracks_per_frame` | 64 | Tracks per frame (up to 10000) |
| `max_datagram_bytes` | 512 | Maximum UDP fragment size (64 tracks → 4 fragments; one frame may use at most 65535 fragments) |
| `max_line_bytes` | 256 | Maximum length of one command line |
| `stats_interval_ms` | 5000 | Periodic stats log (needs `log_level = info`); 0 = off |
| `max_commands_per_sec` | 100 | Per-client TCP command limit, bursts up to the same number; 0 = off. Does not affect the UDP track stream |
| `log_level` | info | debug / info / warn / error / off |

### Client

Demo client (automatically sends REGISTER and START, answers heartbeats, reconnects, counts received fragments
and frames). It runs until Ctrl+C or for `--duration-ms N`:

```bash
./build/radar_client --port 8088
```

Options: `--host`, `--udp-port`, `--duration-ms N` (0 = run until Ctrl+C), `--no-pong` (behave like a zombie),
`--no-start`, `--quiet`, `--reconnect-min-ms N`, `--reconnect-max-ms N`, `--max-retries N`.

The client always reconnects, with no option needed. It retries after a lost connection, a refused connection or
one that takes over 3 s, `ERR FULL`, `ERR BUSY`, or a server that has gone silent (no data within the `timeout_ms` announced in `WELCOME`). Delays
double from `--reconnect-min-ms` (default 1000) up to `--reconnect-max-ms` (default 8000), plus up to 25% random
jitter. Each new connection sends `REGISTER` and `START` again on the same UDP port, and the `SUMMARY` totals
cover the whole run. `--max-retries N` gives up with exit code 3 after N consecutive failures (default 0 = never).

Reconnecting is client-side only. The server treats a reconnect as a new client: the slot number may differ,
its counters start from 0, and frames missed while disconnected are not resent.

You can also type commands by hand with `nc`:

```text
$ nc 127.0.0.1 8088
WELCOME RADARSIM/1.0 slot=0 max=3 hb_ms=1000 timeout_ms=3500
REGISTER 5000
OK REGISTERED udp=127.0.0.1:5000
START
OK STREAMING
HB 1
PONG 1
STATUS
OK STATUS slot=0 state=STREAMING udp_port=5000 frames_sent=61 fragments_sent=244 dropped=0 send_errors=0 throttled=0 queue=0/8
QUIT
BYE
```

### Commands

Each command is one line of text ending in `\n` (`\r\n` is accepted; keywords are case-insensitive).

| Command | Reply | Purpose |
|---|---|---|
| `REGISTER <udp_port>` | `OK REGISTERED udp=<ip>:<port>` | Where to send track data. The IP is always the address the TCP connection came from, so a client cannot aim the stream at a third party |
| `START` | `OK STREAMING` / `ERR NOT_REGISTERED` | Start streaming (requires REGISTER) |
| `STOP` | `OK STOPPED` / `ERR NOT_REGISTERED` | Pause streaming; no datagram is sent after this reply |
| `PING [token]` | `PONG [token]` | Liveness/RTT check from the client side |
| `PONG [seq]` | (none) | Answer to the server's `HB <seq>` heartbeat |
| `STATUS` | `OK STATUS slot= state= udp_port= frames_sent= fragments_sent= dropped= send_errors= throttled= queue=` | Per-client state and counters |
| `QUIT` | `BYE`, then the connection closes | Graceful disconnect |

Each client may send `max_commands_per_sec` commands per second (default 100), with bursts up to the same number. A
client above that is not disconnected and its commands are not lost: the server stops reading from it for a moment,
so its commands are answered later. Only TCP commands are limited; track frames keep arriving over UDP as usual.

Errors: `ERR UNKNOWN_COMMAND <word>`, `ERR BAD_ARGUMENT <reason>` (connection stays open),
`ERR LINE_TOO_LONG` (connection closed). Server-initiated lines: `WELCOME ...` on connect, `ERR FULL` when
all slots are taken, `HB <seq>` every heartbeat, `BYE TIMEOUT` for a zombie, `BYE SHUTDOWN` on server exit, and `ERR BUSY` when the server is out of file descriptors.
The full protocol is in DESIGN.md §6.

### Measurements

The server logs a report every `stats_interval_ms`, including the measured frame period:

```text
stats: clients=1/3 produced=58 dropped=0 rejected_full=0 rejected_busy=0 overruns=0 period_ms avg=35.00 min=34.85 max=35.13 jitter=0.03
stats: slot 0 STREAMING peer=127.0.0.1:34142 udp_port=49649 frames_sent=52 fragments_sent=208 dropped=0 send_errors=0 throttled=0 queue=0 pending_output=0
```

The client reports loss and latency in its `STATS` and `SUMMARY` lines:

```text
SUMMARY fragments=292 frames=73 incomplete=0 invalid=0 heartbeats=2 sessions=1 rejected_full=0 lost=0 loss_pct=0.00 latency_ms_avg=0.10 latency_ms_max=0.17
```

`lost` counts gaps in the frame numbers within a connection, whatever the cause: UDP loss, an incomplete frame, or
a frame the server dropped from a full queue. Latency compares the server's clock with the client's, so it is only
meaningful on one machine or with synchronized clocks.

### Demo scenario

3 clients connect → a 4th is rejected and keeps retrying → one is killed with kill -9 → the waiting client takes
the freed slot → the server restarts while the clients reconnect by themselves:

```bash
bash scripts/demo.sh build
```

## Tests

```bash
cd build
ctest --output-on-failure          # everything (80 unit + 56 integration)
ctest -L unit                       # unit tests only
ctest -L integration                # integration tests only
./integration_tests --gtest_filter='*Kill*'   # a single test (prints its measurements)
```

Required tests from the assignment:

| Requirement | Tests |
|---|---|
| Zombie detection | `Liveness.*`, `ServerIT.FrozenClientIsReapedByHeartbeatTimeout` (SIGSTOP), `ServerIT.SilentClientIsZombieResponsiveClientIsNot` |
| 4 concurrent connections → 4th rejected | `ServerIT.FourthConcurrentClientGetsErrFullAndIsClosed`, `ServerIT.FourSimultaneousConnectsExactlyOneRejected` |
| 3 concurrent streams → fragments received per client | `ServerIT.ThreeClientsStreamConcurrently` |
| Client killed with kill -9 → slot reclaimed | `ServerIT.KillDash9ClientReleasesSlotImmediately`, `ServerIT.KillDash9WhileFullLetsANewClientIn` |

Every other requirement, where it is implemented and how it is checked:

| Requirement | Implementation | Checked by |
|---|---|---|
| TCP command port, default 8088, set in a config file | `src/config.cpp`, [config/server.conf](config/server.conf) | `Config.*`, `tests/fault_test.py` (1) |
| At most 3 clients, set in the config file | `max_clients`, `Server::handle_accept` | `ServerIT.FourthConcurrentClientGetsErrFullAndIsClosed`, `RobustnessIT.LargestAllowedSlotCountWorks` |
| Welcome message on connect | `Server::handle_accept` (`WELCOME ...`) | Read first by every integration test and the smoke test |
| `ERR FULL`, then close, when full | `Server::handle_accept` | `ServerIT.FourthConcurrentClientGetsErrFullAndIsClosed` |
| Line-based commands ending in `\n` | `src/line_buffer.cpp`, `src/protocol.cpp`, `Server::handle_line` | `LineBuffer.*`, `Protocol.*`, `RobustnessIT.CommandSentOneByteAtATime`, `SeveralCommandsInOnePacketAnswerInOrder` |
| Track data per client over UDP, fake frames every 35 ms | `Server::producer_loop`, `Server::sender_loop`, `src/track_frame.cpp` | `ServerIT.ThreeClientsStreamConcurrently`, `ServerIT.FramePeriodIsCloseToConfigured` |
| Heartbeats | `Server::housekeeping` (`HB <seq>`) | `ServerIT.SilentClientIsZombieResponsiveClientIsNot` |
| Zombie cleanup | `src/liveness.hpp`, `Server::housekeeping`, `Server::close_slot` | The zombie and kill -9 tests above |
| Graceful shutdown | `sigwait` in `src/main.cpp`, `Server::stop` | `ServerIT.ServerShutdownSaysByeToEveryone`, smoke test (7) |
| Producer–Consumer pattern | Producer thread → per-client queue → Sender thread | DESIGN.md §3 |
| Bounded queue, overflow policy, drop count | `src/bounded_queue.hpp`, `queue_capacity`, `queue_overflow` | `BoundedQueue.*`, `ServerIT.StuckConsumerDrops*` |
| Socket API chosen, with the reason | POSIX sockets + `poll()` | DESIGN.md §2 |
| Design document: threads and queues, slot state diagram, zombie criteria | [docs/DESIGN.md](docs/DESIGN.md) | §3, §4, §5 |

Beyond the assignment:

- `ReconnectIT.*`: client reconnecting after being reaped as a zombie, retrying a full server, surviving a server
  restart, detecting a silent server, giving up, and Ctrl+C during a backoff.
- `ServerIT.StuckConsumer*`: with one sender held, frames are really dropped and counted, the producer keeps its pace
  (both overflow policies), and other clients lose nothing.
- `ServerIT.ClientThatStopsReadingIsReapedAsStalled`: a client that sends but never reads, caught by the output
  backlog limit rather than the heartbeat timer.
- `ServerIT.FramePeriodIsCloseToConfigured`: the measured frame period.
- `RobustnessIT.*`: connection churn with fd and slot accounting, repeated kill -9, slot reuse, replies surviving a
  client that pauses reading, stopping under load, floods that must not starve other clients, the command rate limit
  (including that a throttled client still receives every track frame), 10 clients competing
  for 3 slots, input edge cases, UDP path changes, and configuration limits.
- `ForgedFragment.*`: UDP datagrams with a huge `total_size` or a header that would leave holes are rejected without
  allocating, and valid frames of every size still get through.
- `*Wraparound*`: frame numbers wrapping from 0xFFFFFFFF to 0.

Sample output from `./integration_tests`:

```text
  client 0: received fragments=168 frames=42 | server sent fragments=168
  client 1: received fragments=168 frames=42 | server sent fragments=168
  client 2: received fragments=168 frames=42 | server sent fragments=168
  slot released 0 ms after SIGKILL
  frozen client reaped 399 ms after SIGSTOP
```

### Python smoke test

Black-box test of the built binaries (`radar_server`, `radar_client`) over real sockets. Uses only the
Python 3 standard library.

```bash
python3 tests/smoke_test.py build
```

It checks 24 items in order: basic commands → 4th connection rejected → 3 concurrent streams (fragments
received = fragments the server sent) → slot reclaimed after a `radar_client` process is killed with
kill -9 → a client that ignores heartbeats is reaped while healthy clients stay → forged UDP datagrams
leave `radar_client` running with its memory unchanged → graceful shutdown on SIGINT. It exits with code 0
when everything passes.

```text
3. 3 clients streaming
  [PASS] client 0: received 172 fragments / 43 frames, server sent 172
  ...
5. zombie: client that never answers heartbeats
  [PASS] zombie gets BYE TIMEOUT  (after 801 ms)
...
24/24 checks passed
```

### Fault injection

```bash
python3 tests/fault_test.py build
```

Runs the real binaries in a private network namespace (`unshare -Urn`), so it needs no sudo, never touches the
machine's real network, and always has port 8088 free. It needs `iproute2` and `iptables`, and is not part of CI
because hosted runners may not allow user namespaces. It checks:

- settings really coming from the config file, and start-up errors
- running out of file descriptors (`ulimit`)
- the server being killed with kill -9, and being frozen with SIGSTOP
- a cable cut (`iptables`): packets vanish and nobody receives a FIN
- packet loss, delay, reordering and duplicates (`tc netem`)

### Sanitizers

```bash
cmake -S . -B build-tsan -DRADAR_SANITIZER=thread -DCMAKE_BUILD_TYPE=Debug
setarch $(uname -m) -R bash -c 'cmake --build build-tsan -j && cd build-tsan && ctest'

cmake -S . -B build-asan -DRADAR_SANITIZER=address,undefined -DCMAKE_BUILD_TYPE=Debug
setarch $(uname -m) -R bash -c 'cmake --build build-asan -j && cd build-asan && ctest'
```

`setarch -R` (ASLR off) works around GCC 11's sanitizers aborting with `unexpected memory mapping` on recent
kernels, including WSL2. `tests/tsan.supp` mutes one known GCC 11 libtsan false positive only; see the
comment in that file.

### Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push to `main` and on pull requests: build, C++
tests and smoke test with g++-9 and g++-13, plus the test suite under both sanitizer builds.

## Layout

```text
src/
  server.{hpp,cpp}       IO / Producer / Sender threads, slot management
  bounded_queue.hpp      bounded queue (drop_oldest / drop_newest, drop counting)
  liveness.hpp           zombie rule
  backoff.hpp            reconnect delay (used by radar_client)
  token_bucket.hpp       per-client command rate limit
  interval_stats.hpp     measured frame period
  stream_stats.hpp       client-side loss and latency
  slot.hpp               slot states, close reasons
  protocol.{hpp,cpp}     command parsing
  line_buffer.{hpp,cpp}  TCP stream → lines
  track_frame.{hpp,cpp}  fake tracks, serialization, UDP fragmentation/reassembly
  config.{hpp,cpp}       config file
  main.cpp               radar_server
tools/radar_client.cpp   demo / test client
tests/unit/              unit tests
tests/integration/       integration tests (real sockets, real child processes)
tests/smoke_test.py      Python smoke test (black-box test of the built binaries)
tests/fault_test.py      Python fault injection (fd limits, server failures, cable cut, netem)
config/server.conf       default config
scripts/demo.sh          end-to-end demo
docs/DESIGN.md           design document (Korean)
```
