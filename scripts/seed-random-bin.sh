#!/usr/bin/env bash
# 生成随机大小的 .bin 文件，直接调用 gos3 管理端上传接口灌入服务。
# 依赖：curl、dd（Linux/WSL/Git Bash 均自带），无需安装 mc。
#
# 使用的接口（HTTP Basic 鉴权）：
#   PUT /gos3/admin/buckets?name={bucket}
#   PUT /gos3/admin/buckets/{bucket}/objects/{object}
set -euo pipefail

ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:19000}"
ACCESS_KEY="${S3_USER:-minioadmin}"
SECRET_KEY="${S3_PASSWORD:-minioadmin}"

BUCKET="${BUCKET:-random-bin}"
PREFIX="${PREFIX:-}"             # 对象前缀，需 URL 安全（不要含空格/中文）

COUNT="${COUNT:-10000}"          # 文件个数
MIN_MB="${MIN_MB:-1}"            # 单文件最小大小（MB）
MAX_MB="${MAX_MB:-10}"           # 单文件最大大小（MB）
CONCURRENCY="${CONCURRENCY:-8}"  # 并发上传数
KEEP_LOCAL="${KEEP_LOCAL:-0}"    # 1 = 保留临时文件
WORK_DIR="${WORK_DIR:-$(mktemp -d)}"

BASE="${ENDPOINT%/}/gos3/admin"
AUTH="$ACCESS_KEY:$SECRET_KEY"

if [ "$MIN_MB" -lt 1 ] || [ "$MAX_MB" -lt "$MIN_MB" ]; then
  echo "非法大小范围: MIN_MB=$MIN_MB MAX_MB=$MAX_MB" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "未找到 curl" >&2
  exit 1
fi

MIN_KB=$((MIN_MB * 1024))
MAX_KB=$((MAX_MB * 1024))
FAILED_FILE="$WORK_DIR/failed.log"

mkdir -p "$WORK_DIR"
: >"$FAILED_FILE"

echo "== gos3 random bin seed =="
echo "endpoint    = $ENDPOINT"
echo "bucket      = $BUCKET"
echo "count       = $COUNT"
echo "size range  = ${MIN_MB}MB ~ ${MAX_MB}MB"
echo "concurrency = $CONCURRENCY"
echo "work dir    = $WORK_DIR"
echo

# 创建 bucket（已存在则忽略，随后用列表接口确认）
curl -sS -o /dev/null -u "$AUTH" -X PUT "$BASE/buckets?name=$BUCKET" || true
if ! curl -sS -u "$AUTH" "$BASE/buckets" | grep -q "\"name\":\"$BUCKET\""; then
  echo "bucket 创建失败: $BUCKET" >&2
  exit 1
fi

upload_one() {
  idx=$1
  key=$(printf 'rand-%06d.bin' "$idx")
  [ -n "$PREFIX" ] && key="$PREFIX/$key"

  if command -v shuf >/dev/null 2>&1; then
    kb=$(shuf -i "$MIN_KB"-"$MAX_KB" -n 1)
  else
    kb=$((MIN_KB + RANDOM % (MAX_KB - MIN_KB + 1)))
  fi

  file="$WORK_DIR/$key"
  mkdir -p "$(dirname "$file")"
  dd if=/dev/urandom of="$file" bs=1024 count="$kb" status=none

  resp="$WORK_DIR/resp-$idx"
  code=$(curl -sS -o "$resp" -w '%{http_code}' \
    -u "$AUTH" -X PUT \
    -H 'Content-Type: application/octet-stream' \
    --data-binary "@$file" \
    "$BASE/buckets/$BUCKET/objects/$key")

  if [ "$code" != "200" ]; then
    printf 'FAIL %s http=%s %s\n' "$key" "$code" "$(cat "$resp" 2>/dev/null)" >>"$FAILED_FILE"
  fi
  rm -f "$file" "$resp"
}
export -f upload_one
export BASE AUTH BUCKET WORK_DIR MIN_KB MAX_KB PREFIX FAILED_FILE

seq 1 "$COUNT" | xargs -P "$CONCURRENCY" -I{} bash -c 'upload_one "$@"' _ {}

FAILED=$(wc -l <"$FAILED_FILE")
OK=$((COUNT - FAILED))
echo
echo "== 上传完成 =="
echo "attempted = $COUNT"
echo "succeeded = $OK"
echo "failed    = $FAILED"

if [ -s "$FAILED_FILE" ]; then
  echo "失败样例：" >&2
  head -n 10 "$FAILED_FILE" >&2
fi

if [ "$KEEP_LOCAL" != "1" ]; then
  rm -rf "$WORK_DIR"
fi

[ "$FAILED" -eq 0 ] || exit 1
echo "任务已完成!kxj"
