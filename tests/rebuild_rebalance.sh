#!/bin/bash

set -e

cargo build --release

DB=/tmp/test.db
REBUILD_DB=/tmp/rebuilt.db
BASE_URL=http://localhost:4000

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <num_keys>"
  exit 1
fi

NUM_KEYS="$1"

start_volume_servers() {
  echo "Starting volume servers..."
  ./scripts/bringup.sh
  sleep 1
}

kill_volume_servers() {
  echo "Stopping nginx volume servers..."
  pkill -f nginx || true
}

start_server() {
  local dbfile="$1"
  echo "Starting crabstore with DB: $dbfile"
  setsid ./target/release/crabstore \
    --pvolumes localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005 \
    --dbfile "$dbfile" \
    run &
  PID=$!
  sleep 1
}

stop_server() {
  echo "Stopping crabstore..."
  kill -- "-$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
}

reset_db() {
  rm -f "$DB" "$REBUILD_DB"
  rm -f /tmp/tmp.*
  rm -rf /tmp/volume*
}

populate() {
  echo "Populating $NUM_KEYS keys..."
  for ((i = 0; i < NUM_KEYS; i++)); do
    key="test-key-$i"
    value="value-$i"
    if ! curl -sL -X PUT \
      --data "$value" \
      "$BASE_URL/$key" >/dev/null; then
      echo "PUT failed for $key"
      return 1
    fi
  done
  echo "Successfully populated $NUM_KEYS keys."
}

rebuild() {
  echo "Rebuilding index..."
  if ! ./target/release/crabstore \
    --pvolumes localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005 \
    --dbfile "$REBUILD_DB" \
    rebuild; then
    echo "Rebuild failed."
    return 1
  fi
  echo "Rebuild completed."
}

rebalance() {
  local pvolumes="$1"
  echo "Rebalancing.."
  if ! ./target/release/crabstore \
    --pvolumes "$pvolumes" \
    --dbfile "$REBUILD_DB" \
    rebalance; then
    echo "rebalance failed."
    return 1
  fi
  echo "rebalance completed."
}

verify() {
  echo "Verifying $NUM_KEYS keys..."
  for ((i = 0; i < NUM_KEYS; i++)); do
    key="test-key-$i"
    expected="value-$i"
    value=$(curl -sL "$BASE_URL/$key") || {
      echo "GET failed for $key"
      return 1
    }
    if [[ "$value" != "$expected" ]]; then
      echo "Value mismatch for $key"
      echo "Expected: $expected"
      echo "Got:      $value"
      return 1
    fi
  done
  echo "All $NUM_KEYS GETs succeeded."
}

main() {
  echo "========================================"
  echo "Running rebuild/rebalance tests"
  echo "========================================"
  reset_db
  start_volume_servers
  trap 'stop_server; kill_volume_servers; reset_db' EXIT
  start_server "$DB"
  populate || return 1
  stop_server
  rebuild || return 1
  start_server "$REBUILD_DB"
  verify || return 1
  stop_server
  rebalance "localhost:4001,localhost:4002,localhost:4003"
  start_server "$REBUILD_DB"
  verify
  stop_server
  rebalance "localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005"
  start_server "$REBUILD_DB"
  verify
  stop_server
  reset_db
  echo "========================================"
  echo "Rebuild/rebalance tests PASSED"
  echo "========================================"
}

main

