#!/usr/bin/env bash
# End-to-end demo with the real binaries:
#   3 clients stream, a 4th is rejected, one client is kill -9'd, a new client
#   takes the freed slot, then the server is stopped with SIGINT.
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

echo "== 4th client (expect rejection) =="
"$BUILD/radar_client" --port "$PORT" --duration-ms 500; echo "exit code: $?"

echo "== kill -9 client 2 (pid $C2) =="
kill -9 "$C2"; wait "$C2" 2>/dev/null
sleep 0.2

echo "== new client takes the freed slot (2 s) =="
"$BUILD/radar_client" --port "$PORT" --duration-ms 2000 --quiet

echo "== SIGINT the server =="
kill -INT "$SERVER"; wait "$SERVER"
sleep 0.3
for i in 1 3; do echo "-- client $i:"; tail -n 3 "$LOG/client$i.log"; done
echo "== server log =="
cat "$LOG/server.log"
rm -rf "$LOG"
