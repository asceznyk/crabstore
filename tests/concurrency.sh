#!/bin/bash
set -e

cargo build

DB=/tmp/test.db

start_volume_servers() {
  echo "Starting volume servers..."
  ./scripts/bringup.sh
}

kill_volume_servers() {
  echo "Stopping nginx volume servers..."
  kill $(pgrep -f nginx)
}

start_server() {
  setsid ./target/debug/crabstore \
    --pvolumes localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005 \
    --dbfile "$DB" \
    run &
  PID=$!
  sleep 1
}

stop_server() {
  kill -- "-$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
}

reset_db() {
  rm -f "$DB"
}

test_concurrent_put() {
  echo "Running concurrent PUT test..."
  results=$(mktemp)
  pids=()
  for i in {1..100}; do
    curl -s -o /dev/null -w "%{http_code}\n" \
      -X PUT --data "value-$i" \
      http://localhost:4000/foo >> "$results" &
    pids+=("$!")
  done
  set +e
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
  set -e
  success=$(grep -c '^2[0-9][0-9]$' "$results" || true)
  forbidden=$(grep -c '^403$' "$results" || true)
  conflicts=$(grep -c '^409$' "$results" || true)
  failures=$((forbidden + conflicts))
  echo "2XX: $success"
  echo "403: $forbidden"
  echo "409: $conflicts"
  if [ "$success" -ne 1 ]; then
    echo "FAIL: expected exactly 1 successful PUT, got $success"
    rm -f "$results"
    return 1
  fi
  if [ "$failures" -ne 99 ]; then
    echo "FAIL: expected 99 failed PUTs, got $failures"
    rm -f "$results"
    return 1
  fi
  rm -f "$results"
  echo "test_concurrent_put PASS"
}

test_concurrent_delete() {
  echo "Running concurrent DELETE test..."
  curl -s -o /dev/null \
    -X PUT \
    --data "value" \
    http://localhost:4000/foo
  results=$(mktemp)
  pids=()
  for i in {1..100}; do
    curl -s -o /dev/null -w "%{http_code}\n" \
      -X DELETE \
      http://localhost:4000/foo >> "$results" &
    pids+=("$!")
  done
  set +e
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
  set -e
  success=$(grep -c '^2[0-9][0-9]$' "$results" || true)
  not_found=$(grep -c '^404$' "$results" || true)
  conflicts=$(grep -c '^409$' "$results" || true)
  failures=$((not_found + conflicts))
  echo "2XX: $success"
  echo "404: $not_found"
  echo "409: $conflicts"
  if [ "$success" -ne 1 ]; then
    echo "FAIL: expected exactly 1 successful DELETE, got $success"
    rm -f "$results"
    return 1
  fi
  if [ "$failures" -ne 99 ]; then
    echo "FAIL: expected 99 failed DELETEs, got $failures"
    rm -f "$results"
    return 1
  fi
  rm -f "$results"
  echo "test_concurrent_delete PASS"
}

test_concurrent_put_delete() {
  echo "Running concurrent PUT/DELETE test..."
  results=$(mktemp)
  pids=()
  for i in {1..100}; do
    curl -s -o /dev/null -w "PUT:%{http_code}\n" \
      -X PUT --data "value-$i" \
      http://localhost:4000/foo >> "$results" &
    pids+=("$!")
    curl -s -o /dev/null -w "DELETE:%{http_code}\n" \
      -X DELETE \
      http://localhost:4000/foo >> "$results" &
    pids+=("$!")
  done
  set +e
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
  set -e
  put_success=$(grep -c '^PUT:2[0-9][0-9]$' "$results" || true)
  put_forbidden=$(grep -c '^PUT:403$' "$results" || true)
  put_conflicts=$(grep -c '^PUT:409$' "$results" || true)
  delete_success=$(grep -c '^DELETE:2[0-9][0-9]$' "$results" || true)
  delete_not_found=$(grep -c '^DELETE:404$' "$results" || true)
  delete_conflicts=$(grep -c '^DELETE:409$' "$results" || true)
  successes=$((put_success + delete_success))
  failures=$((put_forbidden + put_conflicts + delete_not_found + delete_conflicts))
  total=$((successes + failures))
  echo "PUT 2XX:     $put_success"
  echo "PUT 403:     $put_forbidden"
  echo "PUT 409:     $put_conflicts"
  echo "DELETE 2XX:  $delete_success"
  echo "DELETE 404:  $delete_not_found"
  echo "DELETE 409:  $delete_conflicts"
  if [ "$successes" -lt 1 ]; then
    echo "FAIL: expected at least 1 successful PUT or DELETE"
    rm -f "$results"
    return 1
  fi
  if [ "$total" -ne 200 ]; then
    echo "FAIL: expected 200 valid responses, got $total"
    rm -f "$results"
    return 1
  fi
  rm -f "$results"
  echo "PASS"
}

cleanup() {
  echo "Stopping crabstore..."
  stop_server
  reset_db
}

trap cleanup EXIT

start_volume_servers

start_server
test_concurrent_put
stop_server
reset_db

start_server
test_concurrent_delete
stop_server
reset_db

start_server
test_concurrent_put_delete

kill_volume_servers

echo "ALL TESTS PASSED"

