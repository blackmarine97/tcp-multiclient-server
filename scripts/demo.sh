#!/usr/bin/env bash
# End-to-end demo with the real binaries:
#   3 clients stream, a 4th is rejected and keeps retrying, one client is kill -9'd,
#   the waiting client takes the freed slot, and the server restarts while the
#   clients reconnect by themselves.
# Usage: scripts/demo.sh [build_dir]
set -u
BUILD=${1:-build}
PORT=18088
LOG=$(mktemp -d)

"$BUILD/radar_server" -c config/server.conf -p "$PORT" 2> "$LOG/server.log" &
SERVER=$!
sleep 0.3

for i in 1 2 3; do
  "$BUILD/radar_client" --port "$PORT" > "$LOG/client$i.log" 2>&1 &
  eval "C$i=$!"
done
sleep 1

echo "== 4th client: server is full, so it keeps retrying =="
"$BUILD/radar_client" --port "$PORT" --reconnect-min-ms 200 --reconnect-max-ms 400 > "$LOG/client4.log" 2>&1 &
C4=$!
sleep 1
grep -E "ERR FULL|RECONNECT" "$LOG/client4.log" | head -n 4

echo "== kill -9 client 2 (pid $C2): client 4 takes the freed slot =="
kill -9 "$C2"; wait "$C2" 2>/dev/null
sleep 1
grep -E "WELCOME|READY" "$LOG/client4.log"

echo "== restart the server: clients reconnect by themselves =="
kill -INT "$SERVER"; wait "$SERVER"
sleep 0.5
"$BUILD/radar_server" -c config/server.conf -p "$PORT" 2>> "$LOG/server.log" &
SERVER=$!
sleep 3

echo "== stop everything =="
kill -INT "$C1" "$C3" "$C4"; wait "$C1" "$C3" "$C4"
kill -INT "$SERVER"; wait "$SERVER"
for i in 1 3 4; do echo "-- client $i: $(tail -n 1 "$LOG/client$i.log")"; done
echo "== server log =="
cat "$LOG/server.log"
rm -rf "$LOG"
