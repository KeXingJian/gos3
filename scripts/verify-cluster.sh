#!/usr/bin/env bash
set -u

ENDPOINT="${S3_ENDPOINT:-http://node1:9000}"
ACCESS_KEY="${S3_USER:-minioadmin}"
SECRET_KEY="${S3_PASSWORD:-minioadmin}"
PHASE="${PHASE:-write}"
ALIAS="cluster"
BUCKET="cluster-verify"
WORK="$(mktemp -d)"

PASS=0
FAIL=0

ok()  { PASS=$((PASS + 1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL + 1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; }
check() {
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected=[$2] actual=[$3])"; fi
}

echo "== gos3 distributed verification =="
echo "endpoint = $ENDPOINT  phase = $PHASE"

if ! mc alias set "$ALIAS" "$ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null 2>&1; then
  echo "  FAIL alias set - cannot reach node"
  exit 1
fi
ok "alias set"

yes 'gos3-cluster-line' | head -c 8388608 >"$WORK/obj.bin"
WANT="$(md5sum "$WORK/obj.bin" | cut -d' ' -f1)"

if [ "$PHASE" = "write" ]; then
  mc rb --force "$ALIAS/$BUCKET" >/dev/null 2>&1 || true
  if mc mb "$ALIAS/$BUCKET" >/dev/null 2>&1; then ok "make bucket"; else bad "make bucket"; fi
  if mc cp "$WORK/obj.bin" "$ALIAS/$BUCKET/obj.bin" >/dev/null 2>&1; then
    ok "put object across nodes"
  else
    bad "put object across nodes"
  fi
fi

GOT="$(mc cat "$ALIAS/$BUCKET/obj.bin" 2>/dev/null | md5sum | cut -d' ' -f1)"
check "distributed read integrity" "$WANT" "$GOT"

if [ "$PHASE" = "write" ]; then
  if mc ls "$ALIAS/$BUCKET/" 2>/dev/null | grep -q 'obj.bin'; then ok "list object"; else bad "list object"; fi
fi

echo
echo "== result: $PASS passed, $FAIL failed =="
if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
exit 0
