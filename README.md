# RadarSim — 다중접속 트랙 서버 (C++17)

레이더 하드웨어 없이 PC에서 동작하는 다중접속 서버. TCP 명령 포트에 최대 3개 클라이언트가 접속하고,
각 클라이언트는 35 ms마다 생성되는 가짜 트랙 프레임을 UDP로 받는다. 하트비트·좀비 정리·정상 종료를 처리한다.

- 설계 문서: [docs/DESIGN.md](docs/DESIGN.md) — 스레드 구조와 큐, 슬롯 상태도, 좀비 판정 기준과 근거, 소켓 API 선택 이유
- 소켓: POSIX 소켓 + `poll()` (선택 이유는 설계 문서 §2)

## 요구 사항

- Linux (Ubuntu 22.04에서 확인, Windows는 **WSL**로 빌드/실행)
- g++ ≥ 9 (C++17), CMake ≥ 3.16
- GoogleTest: 시스템에 없으면 CMake가 v1.14.0을 자동 다운로드 (인터넷 필요)

```bash
sudo apt install build-essential cmake
```

## 빌드

```bash
cmake -S . -B build
cmake --build build -j
```

결과물: `build/radar_server`, `build/radar_client`, `build/unit_tests`, `build/integration_tests`

## 실행

### 서버

```bash
./build/radar_server -c config/server.conf
```

- `-c <file>`: 설정 파일 (없으면 기본값), `-p <port>`: 포트 덮어쓰기
- Ctrl+C(SIGINT) / SIGTERM → 모든 클라이언트에 `BYE SHUTDOWN` 후 정상 종료

설정 파일 [config/server.conf](config/server.conf):

| 키 | 기본값 | 설명 |
|---|---|---|
| `tcp_port` | 8088 | 명령 포트 (0 = 임시 포트) |
| `max_clients` | 3 | 최대 동시 접속 |
| `frame_interval_ms` | 35 | 프레임 생성 주기 |
| `heartbeat_interval_ms` | 1000 | `HB` 송신 주기 |
| `zombie_timeout_ms` | 3500 | 이 시간 동안 수신이 없으면 좀비로 판정 |
| `queue_capacity` | 8 | 클라이언트별 프레임 큐 크기 |
| `queue_overflow` | drop_oldest | 큐가 가득 찼을 때: `drop_oldest` / `drop_newest` |
| `tracks_per_frame` | 64 | 프레임당 트랙 수 |
| `max_datagram_bytes` | 512 | UDP 조각 최대 크기 (64 트랙 → 4조각) |
| `max_line_bytes` | 256 | 명령 한 줄 최대 길이 |
| `log_level` | info | debug / info / warn / error / off |

### 클라이언트

데모 클라이언트 (자동으로 REGISTER → START, 하트비트 응답, 수신 조각/프레임 집계):

```bash
./build/radar_client --port 8088
```

옵션: `--host`, `--udp-port`, `--duration-ms N`(0 = 무한), `--no-pong`(좀비 흉내), `--no-start`, `--quiet`.
만석이면 `REJECTED: server is full`을 출력하고 종료 코드 2로 끝난다.

`nc`로 직접 명령을 보낼 수도 있다:

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
OK STATUS slot=0 state=STREAMING udp_port=5000 frames_sent=61 fragments_sent=244 dropped=0 queue=0/8
QUIT
BYE
```

명령: `REGISTER <udp_port>`, `START`, `STOP`, `PING [token]`, `PONG [seq]`, `STATUS`, `QUIT` (전체 프로토콜은 설계 문서 §6)

### 데모 시나리오

3개 접속 → 4번째 거부 → 1개 kill -9 → 새 클라이언트 입장 → 서버 SIGINT:

```bash
bash scripts/demo.sh build
```

## 테스트

```bash
cd build
ctest --output-on-failure          # 전체 (unit 43 + integration 14, 약 2초)
ctest -L unit                       # 유닛 테스트만
ctest -L integration                # 통합 테스트만
./integration_tests --gtest_filter='*Kill*'   # 개별 실행 (측정값이 출력됨)
```

과제 요구 테스트와의 대응:

| 요구 | 테스트 |
|---|---|
| 좀비 판정 | `Liveness.*`, `ServerIT.FrozenClientIsReapedByHeartbeatTimeout` (SIGSTOP), `ServerIT.SilentClientIsZombieResponsiveClientIsNot` |
| 4개 동시 접속 → 4번째 거부 | `ServerIT.FourthConcurrentClientGetsErrFullAndIsClosed`, `ServerIT.FourSimultaneousConnectsExactlyOneRejected` |
| 3개 동시 스트리밍 → 클라이언트별 수신 조각 수 | `ServerIT.ThreeClientsStreamConcurrently` |
| kill -9 → 슬롯 회수 | `ServerIT.KillDash9ClientReleasesSlotImmediately`, `ServerIT.KillDash9WhileFullLetsANewClientIn` |

실행 예 (`./integration_tests`의 출력 일부):

```text
  client 0: received fragments=168 frames=42 | server sent fragments=168
  client 1: received fragments=168 frames=42 | server sent fragments=168
  client 2: received fragments=168 frames=42 | server sent fragments=168
  slot released 0 ms after SIGKILL
  frozen client reaped 399 ms after SIGSTOP
```

### ThreadSanitizer

```bash
cmake -S . -B build-tsan -DRADAR_SANITIZER=thread -DCMAKE_BUILD_TYPE=Debug
setarch $(uname -m) -R bash -c 'cmake --build build-tsan -j && cd build-tsan && ctest'
```

`setarch -R`(ASLR 끄기)은 GCC 11 TSan이 최신 커널(WSL2 포함)에서 `unexpected memory mapping`으로 죽는 문제의 우회책이다.
`tests/tsan.supp`는 GCC 11 libtsan의 알려진 오탐 1종만 억제한다(자세한 내용은 파일 주석).

## 디렉터리 구조

```text
src/
  server.{hpp,cpp}       IO / Producer / Sender 스레드, 슬롯 관리
  bounded_queue.hpp      크기 제한 큐 (drop_oldest / drop_newest, 드롭 집계)
  liveness.hpp           좀비 판정 규칙
  slot.hpp               슬롯 상태, 종료 사유
  protocol.{hpp,cpp}     명령 파싱
  line_buffer.{hpp,cpp}  TCP 스트림 → 줄 분리
  track_frame.{hpp,cpp}  가짜 트랙 생성, 직렬화, UDP 조각화/재조립
  config.{hpp,cpp}       설정 파일
  main.cpp               radar_server
tools/radar_client.cpp   데모/테스트 클라이언트
tests/unit/              유닛 테스트
tests/integration/       통합 테스트 (실제 소켓, 실제 자식 프로세스)
config/server.conf       기본 설정
scripts/demo.sh          E2E 데모
docs/DESIGN.md           설계 문서
```
