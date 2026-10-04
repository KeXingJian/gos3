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

if [ "${ERASURE_DRIVES:-0}" -ge 2 ] && [ -d /data/d2 ]; then
  yes 'gos3-erasure-line' | head -c 8388608 > "$WORK/erasure.bin"
  E_MD5="$(md5sum "$WORK/erasure.bin" | cut -d' ' -f1)"
  if mc cp "$WORK/erasure.bin" "$ALIAS/$BUCKET/erasure.bin" >/dev/null 2>&1; then
    ok "erasure seed object"
  else
    bad "erasure seed object"
  fi
  rm -f "/data/d2/.data/$BUCKET/erasure.bin/null"
  GOT="$(mc cat "$ALIAS/$BUCKET/erasure.bin" 2>/dev/null | md5sum | cut -d' ' -f1)"
  check "erasure read after losing drive d2" "$E_MD5" "$GOT"
  rm -f "/data/d3/.data/$BUCKET/erasure.bin/null"
  GOT="$(mc cat "$ALIAS/$BUCKET/erasure.bin" 2>/dev/null | md5sum | cut -d' ' -f1)"
  check "erasure read after losing drives d2+d3" "$E_MD5" "$GOT"
  rm -f "/data/d4/.data/$BUCKET/erasure.bin/null"
  if mc cat "$ALIAS/$BUCKET/erasure.bin" >/dev/null 2>&1; then
    bad "erasure read with insufficient shards should fail"
  else
    ok "erasure read with insufficient shards rejected"
  fi
fi

if mc rm "$ALIAS/$BUCKET/small.txt" "$ALIAS/$BUCKET/large.bin" "$ALIAS/$BUCKET/share.txt" "$ALIAS/$BUCKET/erasure.bin" >/dev/null 2>&1; then
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

VBUCKET="$BUCKET-ver"
mc rb --force "$ALIAS/$VBUCKET" >/dev/null 2>&1 || true
mc mb "$ALIAS/$VBUCKET" >/dev/null 2>&1
if mc version enable "$ALIAS/$VBUCKET" >/dev/null 2>&1; then ok "versioning enable"; else bad "versioning enable"; fi
if mc version info "$ALIAS/$VBUCKET" 2>/dev/null | grep -qi "enabled"; then ok "versioning status enabled"; else bad "versioning status enabled"; fi

printf 'v1\n' > "$WORK/v1.txt"
printf 'v2\n' > "$WORK/v2.txt"
mc cp "$WORK/v1.txt" "$ALIAS/$VBUCKET/v.txt" >/dev/null 2>&1
mc cp "$WORK/v2.txt" "$ALIAS/$VBUCKET/v.txt" >/dev/null 2>&1
VCOUNT="$(mc ls --versions "$ALIAS/$VBUCKET/" 2>/dev/null | grep -c 'v.txt')"
if [ "$VCOUNT" -ge 2 ]; then ok "two versions stored"; else bad "two versions stored (got $VCOUNT)"; fi
check "latest version content" "v2" "$(mc cat "$ALIAS/$VBUCKET/v.txt" 2>/dev/null)"

mc rm "$ALIAS/$VBUCKET/v.txt" >/dev/null 2>&1
if mc ls "$ALIAS/$VBUCKET/" 2>/dev/null | grep -q 'v.txt'; then bad "delete marker hides latest"; else ok "delete marker hides latest"; fi
VCOUNT="$(mc ls --versions "$ALIAS/$VBUCKET/" 2>/dev/null | grep -c 'v.txt')"
if [ "$VCOUNT" -ge 3 ]; then ok "versions retained after delete"; else bad "versions retained after delete (got $VCOUNT)"; fi

VID="$(mc ls --versions --json "$ALIAS/$VBUCKET/" 2>/dev/null | sed -n 's/.*"versionId":"\([a-f0-9][a-f0-9]*\)".*/\1/p' | head -1)"
if [ -n "$VID" ]; then
  BEFORE="$(mc ls --versions "$ALIAS/$VBUCKET/" 2>/dev/null | grep -c 'v.txt')"
  mc rm --version-id "$VID" "$ALIAS/$VBUCKET/v.txt" >/dev/null 2>&1
  AFTER="$(mc ls --versions "$ALIAS/$VBUCKET/" 2>/dev/null | grep -c 'v.txt')"
  if [ "$AFTER" -lt "$BEFORE" ]; then ok "permanent delete of a version"; else bad "permanent delete of a version"; fi
else
  bad "extract version id"
fi

if mc version suspend "$ALIAS/$VBUCKET" >/dev/null 2>&1; then ok "versioning suspend"; else bad "versioning suspend"; fi

mc rm --recursive --force --versions "$ALIAS/$VBUCKET" >/dev/null 2>&1 || true
mc rb --force "$ALIAS/$VBUCKET" >/dev/null 2>&1 || true

ADMIN="http://gos3:9000/gos3/admin"
IBUCKET="$BUCKET-iam"
mc rb --force "$ALIAS/$IBUCKET" >/dev/null 2>&1 || true
mc mb "$ALIAS/$IBUCKET" >/dev/null 2>&1
mc cp "$WORK/small.txt" "$ALIAS/$IBUCKET/hello.txt" >/dev/null 2>&1

curl -s -u minioadmin:minioadmin -X PUT "$ADMIN/users?accessKey=alice&secretKey=alice123" >/dev/null
cat >"$WORK/ro.json" <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket","s3:ListAllMyBuckets"],"Resource":["arn:aws:s3:::$IBUCKET","arn:aws:s3:::$IBUCKET/*","*"]}]}
EOF
curl -s -u minioadmin:minioadmin -X PUT --data-binary @"$WORK/ro.json" "$ADMIN/policies?name=readonly" >/dev/null
curl -s -u minioadmin:minioadmin -X PUT "$ADMIN/attach?accessKey=alice&policy=readonly" >/dev/null

mc alias set alice http://gos3:9000 alice alice123 >/dev/null 2>&1
check "iam read allowed" "hello gos3" "$(mc cat alice/$IBUCKET/hello.txt 2>/dev/null)"
if mc cp "$WORK/small.txt" "alice/$IBUCKET/denied.txt" >/dev/null 2>&1; then
  bad "iam write denied"
else
  ok "iam write denied"
fi

LBUCKET="$BUCKET-life"
mc rb --force "$ALIAS/$LBUCKET" >/dev/null 2>&1 || true
mc mb "$ALIAS/$LBUCKET" >/dev/null 2>&1
mc cp "$WORK/small.txt" "$ALIAS/$LBUCKET/expire.txt" >/dev/null 2>&1
if mc ilm rule add --expire-days 1 "$ALIAS/$LBUCKET" >/dev/null 2>&1; then ok "lifecycle rule add"; else bad "lifecycle rule add"; fi
if mc ilm rule ls "$ALIAS/$LBUCKET" 2>/dev/null | grep -qi 'enabled'; then ok "lifecycle rule listed"; else bad "lifecycle rule listed"; fi

OLDTS="$(date -u -d '2 days ago' +%Y-%m-%dT%H:%M:%SZ)"
for d in d1 d2 d3 d4; do
  f="/data/$d/.meta/$LBUCKET/expire.txt.json"
  [ -f "$f" ] && sed -i "s/\"modTime\":\"[^\"]*\"/\"modTime\":\"$OLDTS\"/" "$f"
done
sleep 7
if mc ls "$ALIAS/$LBUCKET/" 2>/dev/null | grep -q 'expire.txt'; then
  bad "lifecycle expiration deletes object"
else
  ok "lifecycle expiration deletes object"
fi

curl -s -u minioadmin:minioadmin -X DELETE "$ADMIN/users?accessKey=alice" >/dev/null
mc rb --force "$ALIAS/$IBUCKET" >/dev/null 2>&1 || true
mc rb --force "$ALIAS/$LBUCKET" >/dev/null 2>&1 || true

echo
echo "== result: $PASS passed, $FAIL failed =="
if [ "$FAIL" -gt 0 ]; then
  printf 'failed cases:\n'
  for f in "${FAILED[@]}"; do printf '  - %s\n' "$f"; done
  exit 1
fi
exit 0
