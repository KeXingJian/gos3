#!/usr/bin/env bash
set -u

ENDPOINT="${S3_ENDPOINT:-http://gos3:9000}"
ACCESS_KEY="${S3_USER:-minioadmin}"
SECRET_KEY="${S3_PASSWORD:-minioadmin}"
ALIAS="verify"
BUCKET="gos3-verify"
WORK="$(mktemp -d)"

PASS=0
FAIL=0
FAILED=()

ok()   { PASS=$((PASS + 1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad()  { FAIL=$((FAIL + 1)); FAILED+=("$1"); printf '  \033[31mFAIL\033[0m %s\n' "$1"; }
check() {
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected=[$2] actual=[$3])"; fi
}

echo "== gos3 verification =="
echo "endpoint = $ENDPOINT"
echo

if ! mc alias set "$ALIAS" "$ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null 2>&1; then
  echo "  FAIL alias set - cannot reach server or signature rejected"
  exit 1
fi
ok "alias set + signature accepted"

mc rb --force "$ALIAS/$BUCKET" >/dev/null 2>&1 || true

if mc mb "$ALIAS/$BUCKET" >/dev/null 2>&1; then ok "make bucket"; else bad "make bucket"; fi

printf 'hello gos3\n' > "$WORK/small.txt"
if mc cp "$WORK/small.txt" "$ALIAS/$BUCKET/small.txt" >/dev/null 2>&1; then
  ok "put small object"
else
  bad "put small object"
fi

CONTENT="$(mc cat "$ALIAS/$BUCKET/small.txt" 2>/dev/null)"
check "get small object content" "hello gos3" "$CONTENT"

if mc stat "$ALIAS/$BUCKET/small.txt" >/dev/null 2>&1; then ok "stat object"; else bad "stat object"; fi

if mc ls "$ALIAS/$BUCKET" 2>/dev/null | grep -q 'small.txt'; then ok "list objects"; else bad "list objects"; fi

dd if=/dev/urandom of="$WORK/large.bin" bs=1M count=20 >/dev/null 2>&1
ORIG_MD5="$(md5sum "$WORK/large.bin" | cut -d' ' -f1)"
if mc cp --debug "$WORK/large.bin" "$ALIAS/$BUCKET/large.bin" >"$WORK/cp.log" 2>&1; then
  ok "put large object"
else
  bad "put large object"
fi
if grep -q 'uploads' "$WORK/cp.log"; then
  ok "multipart initiate observed"
else
  bad "multipart initiate not observed"
fi
mc cat "$ALIAS/$BUCKET/large.bin" > "$WORK/large.out" 2>/dev/null || true
GOT_MD5="$(md5sum "$WORK/large.out" | cut -d' ' -f1)"
check "multipart content integrity" "$ORIG_MD5" "$GOT_MD5"

ANON_CODE="$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/" || echo 000)"
check "anonymous request denied" "403" "$ANON_CODE"

HEALTH_CODE="$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/healthz" || echo 000)"
check "health endpoint public" "200" "$HEALTH_CODE"

mc cp "$WORK/small.txt" "$ALIAS/$BUCKET/share.txt" >/dev/null 2>&1
SHARE_URL="$(mc share download --expire 1h --json "$ALIAS/$BUCKET/share.txt" 2>/dev/null | sed -n 's/.*"share":"\([^"]*\)".*/\1/p')"
if [ -n "$SHARE_URL" ]; then
  SHARE_BODY="$(curl -s "$SHARE_URL" || true)"
  check "presigned URL GET" "hello gos3" "$SHARE_BODY"
else
  bad "presigned URL generation"
fi

if mc rm "$ALIAS/$BUCKET/small.txt" "$ALIAS/$BUCKET/large.bin" "$ALIAS/$BUCKET/share.txt" >/dev/null 2>&1; then
  ok "delete objects"
else
  bad "delete objects"
fi
if mc rb "$ALIAS/$BUCKET" >/dev/null 2>&1; then ok "remove empty bucket"; else bad "remove empty bucket"; fi

mc mb "$ALIAS/$BUCKET" >/dev/null 2>&1
mc cp "$WORK/small.txt" "$ALIAS/$BUCKET/x.txt" >/dev/null 2>&1
if mc rb "$ALIAS/$BUCKET" >/dev/null 2>&1; then
  bad "non-empty bucket must be rejected"
else
  ok "non-empty bucket rejected"
fi
mc rm --force "$ALIAS/$BUCKET/x.txt" >/dev/null 2>&1 || true
mc rb "$ALIAS/$BUCKET" >/dev/null 2>&1 || true

echo
echo "== result: $PASS passed, $FAIL failed =="
if [ "$FAIL" -gt 0 ]; then
  printf 'failed cases:\n'
  for f in "${FAILED[@]}"; do printf '  - %s\n' "$f"; done
  exit 1
fi
exit 0
