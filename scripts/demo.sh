#!/usr/bin/env bash
# End-to-end demo against the two docker-compose instances.
# Usage: ./scripts/demo.sh   (expects api-a on :8001 and api-b on :8002)
set -euo pipefail

API_A="${API_A:-http://localhost:8001}"
API_B="${API_B:-http://localhost:8002}"
SAMPLES="$(cd "$(dirname "$0")/.." && pwd)/samples"
METER="meter-east-01"
T0=1793491200   # 2026-11-01T00:00:00Z
T5=1793509200   # 2026-11-01T05:00:00Z

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

# post_batch <api> <csv-file>: prints "HTTP <code>" plus the pretty JSON body.
# On success the new batch_id is stored in the global LAST_BATCH_ID.
post_batch() {
  local out body
  out="$(curl -s -w '\n%{http_code}' -F "file=@$2;type=text/csv" "$1/batches")"
  echo "  HTTP $(echo "$out" | tail -1)"
  body="$(echo "$out" | head -n -1)"
  echo "$body" | jq . | sed 's/^/  /'
  LAST_BATCH_ID="$(echo "$body" | jq -r '.batch_id // empty')"
}

# download_and_verify <api> <batch_id> <original-csv-file>
download_and_verify() {
  local api="$1" bid="$2" original="$3" out headers recorded actual
  out="$(mktemp)"
  headers="$(mktemp)"
  curl -s -D "$headers" -o "$out" "$api/batches/$bid/download"
  recorded="$(grep -i '^x-content-sha256:' "$headers" | tr -d '\r' | cut -d' ' -f2)"
  actual="$(sha256sum "$out" | cut -d' ' -f1)"
  if cmp -s "$out" "$original" && [ "$actual" = "$recorded" ]; then
    echo "  download OK: byte-identical to $original, SHA-256 $actual"
  else
    echo "  download FAILED for $bid"
    rm -f "$out" "$headers"
    exit 1
  fi
  rm -f "$out" "$headers"
}

query_hours() {
  curl -s "$1/hourly-usage?meter_id=$2&from_second=$T0&to_second=$T5" |
    jq -r '.[] | "  \(.hour_start_utc)  billable=\(.billable)  kwh_used=\(.kwh_used)  reason=\(.reason)"'
}

say "0. wait for both instances"
for api in "$API_A" "$API_B"; do
  for i in $(seq 1 60); do
    curl -sf "$api/health" >/dev/null 2>&1 && break
    [ "$i" = 60 ] && { echo "$api not healthy"; exit 1; }
    sleep 1
  done
  echo "  $api healthy"
done

say "1. api-a <- batch1 (backfill with a 2-hour gap: readings at 00,01,02,05)"
post_batch "$API_A" "$SAMPLES/batch1_initial_with_gap.csv"
BID1="$LAST_BATCH_ID"
echo "  batch1 batch_id: $BID1"
echo "hourly usage of $METER (hours 02,03,04 must be unbillable gap):"
query_hours "$API_A" "$METER"

say "2. api-b <- batch2 (LATE readings 03,04 fill the gap; neighbours update together)"
post_batch "$API_B" "$SAMPLES/batch2_late_gap_fill.csv"
BID2="$LAST_BATCH_ID"
echo "hourly usage after the late fill (all hours billable now):"
query_hours "$API_B" "$METER"

say "3. api-a <- batch3 (identical retransmission: recorded once, deduplicated, own archive)"
post_batch "$API_A" "$SAMPLES/batch3_retransmit_same_values.csv"
BID3="$LAST_BATCH_ID"
echo "  retransmit batch_id: $BID3 (each accepted batch, even fully deduplicated ones, keeps its own original)"
echo "raw readings of $METER (still one row per second):"
curl -s "$API_A/readings?meter_id=$METER&from_second=$T0&to_second=$T5" |
  jq -r '.[] | "  \(.utc_time)  kwh=\(.kwh)"'

say "4. api-b <- batch4 (conflicting value for an existing second: whole batch rejected)"
post_batch "$API_B" "$SAMPLES/batch4_conflicting_value.csv"
echo "state after the rejected batch (unchanged):"
query_hours "$API_A" "$METER"
n_archives="$(curl -s "$API_A/batches" | jq 'length')"
echo "  accepted batches so far: $n_archives (a rejected batch leaves no row and no archived file)"

say "4b. raw-byte archive audit: download originals back and verify SHA-256"
# Download via the OTHER instance than the one that received each upload.
download_and_verify "$API_B" "$BID1" "$SAMPLES/batch1_initial_with_gap.csv"
download_and_verify "$API_A" "$BID2" "$SAMPLES/batch2_late_gap_fill.csv"
download_and_verify "$API_B" "$BID3" "$SAMPLES/batch3_retransmit_same_values.csv"
echo "  audit list (each accepted batch carries sha256/size and downloadable=true):"
curl -s "$API_A/batches" |
  jq -r '.[] | "  \(.batch_id)  downloadable=\(.downloadable)  sha256=\(.archive_sha256[0:16])...  size=\(.archive_size)"'
echo "  unknown batch_id -> HTTP $(curl -s -o /dev/null -w '%{http_code}' "$API_A/batches/00000000000000000000000000000000/download") (404)"

say "5. api-a <- batch5 (meter-west-02 rolls back 860 -> 855)"
post_batch "$API_A" "$SAMPLES/batch5_meter_rollback.csv"
echo "hourly usage of meter-west-02 (hour 00 unbillable rollback, hour 01 billable):"
query_hours "$API_A" "meter-west-02"

say "6. concurrency: api-a and api-b backfill meter-north-03 at the same time"
post_batch "$API_A" "$SAMPLES/batch6a_concurrent.csv" > /tmp/demo_cc_a.txt &
PID_A=$!
post_batch "$API_B" "$SAMPLES/batch6b_concurrent.csv" > /tmp/demo_cc_b.txt &
PID_B=$!
wait "$PID_A" "$PID_B"
echo "api-a:"; cat /tmp/demo_cc_a.txt
echo "api-b:"; cat /tmp/demo_cc_b.txt
echo "all readings of meter-north-03 (both successful batches preserved):"
curl -s "$API_A/readings?meter_id=meter-north-03" | jq -r '.[] | "  \(.utc_time)  kwh=\(.kwh)"'
echo "hourly usage of meter-north-03 (consistent with the final readings):"
query_hours "$API_B" "meter-north-03"

say "7. final archive audit: every accepted batch is downloadable and hash-verifiable"
curl -s "$API_A/batches" | jq -c '.[] | {batch_id, downloadable, archive_size}'
not_downloadable="$(curl -s "$API_A/batches" | jq '[.[] | select(.downloadable == false)] | length')"
[ "$not_downloadable" = "0" ] && echo "  all accepted batches have downloadable, verified originals"

say "demo finished"
