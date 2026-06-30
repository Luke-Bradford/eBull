#!/usr/bin/env bash
# Unit test for supervisor.sh reset-aware usage-limit backoff.
# Sources the REAL supervisor.sh (main loop guarded by BASH_SOURCE==$0, so
# sourcing only defines functions) and exercises:
#   - extract_reset_epoch():  parse the API reset time from a rejected
#                             rate_limit_event, across field-name/format variants.
#   - is_usage_limit_hit():   the blocked/not-blocked classifier (overage + a
#                             successful terminal result are NOT blocks).
#   - compute_limit_wait():   sane-future bounding of the persisted reset marker.
#
# Run:  bash scripts/autonomy/test_usage_limit_reset.sh   (exit 0 = all pass)

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$HERE/supervisor.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
RESET_STATE="$tmp/.last_usage_reset"   # override: don't touch the real marker

fails=0
check() { # check <desc> <expected> <actual>
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}
between() { # between <desc> <lo> <hi> <actual>
  if [ -n "$4" ] && [ "$4" -ge "$2" ] && [ "$4" -le "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (want [$2,$3], got '$4')"; fails=$((fails + 1)); fi
}

mklog() { printf '%s\n' "$1" > "$tmp/log.jsonl"; echo "$tmp/log.jsonl"; }
ISO_EPOCH="$(python3 -c 'from datetime import datetime,timezone;print(int(datetime(2030,6,30,12,0,0,tzinfo=timezone.utc).timestamp()))')"

# --- extract_reset_epoch: field-name / format variants (defensive parse) ---
f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":"2030-06-30T12:00:00Z"}}')"
check "ISO-8601 'resetsAt' → epoch" "$ISO_EPOCH" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","reset":4102444800}}')"
check "epoch-seconds 'reset'" 4102444800 "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetAt":4102444800000}}')"
check "epoch-millis 'resetAt' → seconds" 4102444800 "$(extract_reset_epoch "$f")"

now="$(date +%s)"
f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","retryAfter":1800}}')"
between "relative 'retryAfter' (number) → now+secs" "$((now + 1790))" "$((now + 1815))" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","retryAfter":"1800"}}')"
between "relative 'retryAfter' (string) → now+secs" "$((now + 1790))" "$((now + 1815))" "$(extract_reset_epoch "$f")"

# --- extract_reset_epoch: must IGNORE non-blocking / irrelevant events ---
f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","isUsingOverage":true,"resetsAt":"2030-06-30T12:00:00Z"}}')"
check "overage-covered rejection yields no reset" "" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","resetsAt":"2030-06-30T12:00:00Z"}}')"
check "non-rejected event yields no reset" "" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"assistant","message":{"content":"rate limit reset at 9999999999"}}')"
check "content text is never parsed (#1770)" "" "$(extract_reset_epoch "$f")"

# --- is_usage_limit_hit: classifier sanity (blocked vs not) ---
f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}')"
if is_usage_limit_hit "$f"; then r=blocked; else r=ok; fi
check "rejected + no terminal result = blocked" blocked "$r"

printf '%s\n%s\n' \
  '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}' \
  '{"type":"result","is_error":false}' > "$tmp/log.jsonl"
if is_usage_limit_hit "$tmp/log.jsonl"; then r=blocked; else r=ok; fi
check "rejected BUT session succeeded = not blocked" ok "$r"

# --- compute_limit_wait: bounding of the persisted marker ---
now="$(date +%s)"
echo "$((now + 600))" > "$RESET_STATE"
if w="$(compute_limit_wait)"; then rc=0; else rc=1; fi
check "future reset returns exit 0" 0 "$rc"
between "future reset wait ≈ remaining" 590 600 "$w"

echo "$((now - 60))" > "$RESET_STATE"
if compute_limit_wait >/dev/null; then rc=0; else rc=1; fi
check "past reset → fallback (exit 1)" 1 "$rc"

echo "$((now + LIMIT_RESET_MAX_HORIZON + 3600))" > "$RESET_STATE"
if compute_limit_wait >/dev/null; then rc=0; else rc=1; fi
check "implausibly-far reset → fallback (exit 1)" 1 "$rc"

echo "not-a-number" > "$RESET_STATE"
if compute_limit_wait >/dev/null; then rc=0; else rc=1; fi
check "garbage marker → fallback (exit 1)" 1 "$rc"

rm -f "$RESET_STATE"
if compute_limit_wait >/dev/null; then rc=0; else rc=1; fi
check "missing marker → fallback (exit 1)" 1 "$rc"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
