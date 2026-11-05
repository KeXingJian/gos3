package catalog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"time"
)

// castagnoli 是 crc32.Castagnoli 多项式表，用于所有记录/快照的校验。
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCRC 表示校验和不匹配（数据损坏），区别于「记录不完整」的 torn tail。
// WAL 重放时据此决定：损坏 → 触发全量重建；不完整 → 忽略尾部。
var ErrCRC = errors.New("codec: crc mismatch")

const (
	docFlagIsLatest     = 1 << 0
	docFlagDeleteMarker = 1 << 1
)

// WAL 记录里的 op 字节。
const (
	walOpPut byte = 0x01
	walOpDel byte = 0x02
)

// ---- 基础编解码原语 ----

func appendUvarint(buf []byte, v uint64) []byte {
	return binary.AppendUvarint(buf, v)
}

func consumeUvarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, data, fmt.Errorf("codec: bad uvarint (n=%d)", n)
	}
	return v, data[n:], nil
}

// appendVarint 用 zigzag 编码有符号整数（size / modTime）。
func appendVarint(buf []byte, v int64) []byte {
	u := uint64(v<<1) ^ uint64(v>>63)
	return binary.AppendUvarint(buf, u)
}

func consumeVarint(data []byte) (int64, []byte, error) {
	u, rest, err := consumeUvarint(data)
	if err != nil {
		return 0, data, err
	}
	return int64(u>>1) ^ -int64(u&1), rest, nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func consumeString(data []byte) (string, []byte, error) {
	n, rest, err := consumeUvarint(data)
	if err != nil {
		return "", data, err
	}
	if n > uint64(len(rest)) {
		return "", data, fmt.Errorf("codec: string len %d exceeds %d bytes", n, len(rest))
	}
	return string(rest[:n]), rest[n:], nil
}

func consumeByte(data []byte) (byte, []byte, error) {
	if len(data) < 1 {
		return 0, data, errors.New("codec: need 1 byte")
	}
	return data[0], data[1:], nil
}

// appendMap 按键排序编码 string->string，保证编码确定（快照可复现、crc 稳定）。
func appendMap(buf []byte, m map[string]string) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendString(buf, k)
		buf = appendString(buf, m[k])
	}
	return buf
}

func consumeMap(data []byte) (map[string]string, []byte, error) {
	n, rest, err := consumeUvarint(data)
	if err != nil {
		return nil, data, err
	}
	m := make(map[string]string, n)
	for i := uint64(0); i < n; i++ {
		var k, v string
		if k, rest, err = consumeString(rest); err != nil {
			return nil, data, err
		}
		if v, rest, err = consumeString(rest); err != nil {
			return nil, data, err
		}
		m[k] = v
	}
	return m, rest, nil
}

// ---- Doc 编码（固定字段顺序） ----

// encodeDoc 编码完整 Doc（OpPut 用）。
func encodeDoc(d *Doc) []byte {
	buf := make([]byte, 0, 64)
	buf = appendString(buf, d.Bucket)
	buf = appendString(buf, d.Key)
	buf = appendString(buf, d.VersionID)
	buf = appendString(buf, d.ETag)
	buf = appendString(buf, d.ContentType)
	var flags byte
	if d.IsLatest {
		flags |= docFlagIsLatest
	}
	if d.DeleteMarker {
		flags |= docFlagDeleteMarker
	}
	buf = append(buf, flags)
	buf = appendVarint(buf, d.Size)
	nano := d.ModTime.UnixNano()
	if d.ModTime.IsZero() {
		nano = 0
	}
	buf = appendVarint(buf, nano)
	buf = appendMap(buf, d.UserMeta)
	buf = appendMap(buf, d.Tags)
	return buf
}

// encodeKeyOnly 编码 OpDel 用的最小 Doc（只有 bucket/key/versionID）。
func encodeKeyOnly(d *Doc) []byte {
	buf := make([]byte, 0, 32)
	buf = appendString(buf, d.Bucket)
	buf = appendString(buf, d.Key)
	buf = appendString(buf, d.VersionID)
	return buf
}

// decodeDoc 解码完整 Doc（OpPut 用）。
func decodeDoc(data []byte) (*Doc, error) {
	d := &Doc{}
	var err error
	var flags byte
	if d.Bucket, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if d.Key, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if d.VersionID, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if d.ETag, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if d.ContentType, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if flags, data, err = consumeByte(data); err != nil {
		return nil, err
	}
	d.IsLatest = flags&docFlagIsLatest != 0
	d.DeleteMarker = flags&docFlagDeleteMarker != 0
	if d.Size, data, err = consumeVarint(data); err != nil {
		return nil, err
	}
	var nano int64
	if nano, data, err = consumeVarint(data); err != nil {
		return nil, err
	}
	if nano != 0 {
		d.ModTime = time.Unix(0, nano).UTC()
	}
	if d.UserMeta, data, err = consumeMap(data); err != nil {
		return nil, err
	}
	if d.Tags, data, err = consumeMap(data); err != nil {
		return nil, err
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("codec: doc has %d trailing bytes", len(data))
	}
	return d, nil
}

// decodeKeyOnly 解码 OpDel 用的最小 Doc。
func decodeKeyOnly(data []byte) (*Doc, error) {
	d := &Doc{}
	var err error
	if d.Bucket, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if d.Key, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if d.VersionID, data, err = consumeString(data); err != nil {
		return nil, err
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("codec: key-only doc has %d trailing bytes", len(data))
	}
	return d, nil
}

// ---- WAL 记录 ----

// appendWALRecord 追加一条 WAL 记录：[seq][crc32][op][docLen][doc]。
// crc 只覆盖 op 及之后的部分；seq 不在 crc 内，靠重放时的连续性检查兜底。
func appendWALRecord(buf []byte, seq uint64, op Op, d *Doc) []byte {
	var opByte byte
	var docBytes []byte
	if op == OpDel {
		opByte = walOpDel
		docBytes = encodeKeyOnly(d)
	} else {
		opByte = walOpPut
		docBytes = encodeDoc(d)
	}
	payload := make([]byte, 0, 1+binary.MaxVarintLen64+len(docBytes))
	payload = append(payload, opByte)
	payload = appendUvarint(payload, uint64(len(docBytes)))
	payload = append(payload, docBytes...)

	buf = appendUvarint(buf, seq)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(payload, castagnoli))
	buf = append(buf, payload...)
	return buf
}

// decodeWALRecord 解码一条 WAL 记录，返回 seq、op、doc 与消费的字节数。
// 记录无长度前缀，须先解析 op+docLen 确定本记录边界后，再校验 crc（crc 只覆盖本记录）。
func decodeWALRecord(data []byte) (seq uint64, op Op, doc *Doc, n int, err error) {
	start := data
	if seq, data, err = consumeUvarint(data); err != nil {
		return 0, 0, nil, 0, err
	}
	if len(data) < 4 {
		return 0, 0, nil, 0, errors.New("codec: wal record too short for crc")
	}
	wantCrc := binary.LittleEndian.Uint32(data[:4])
	payload := data[4:]

	if len(payload) < 1 {
		return 0, 0, nil, 0, errors.New("codec: wal payload missing op")
	}
	opByte := payload[0]
	var docLen uint64
	var afterDocLen []byte
	if docLen, afterDocLen, err = consumeUvarint(payload[1:]); err != nil {
		return 0, 0, nil, 0, err
	}
	docLenBytes := len(payload[1:]) - len(afterDocLen)
	payloadLen := 1 + docLenBytes + int(docLen)
	if payloadLen > len(payload) {
		return 0, 0, nil, 0, fmt.Errorf("codec: wal record payload len %d exceeds %d", payloadLen, len(payload))
	}
	actual := payload[:payloadLen]
	if crc32.Checksum(actual, castagnoli) != wantCrc {
		return 0, 0, nil, 0, fmt.Errorf("%w: wal", ErrCRC)
	}

	docBytes := actual[1+docLenBytes:]
	switch opByte {
	case walOpPut:
		op = OpPut
		doc, err = decodeDoc(docBytes)
	case walOpDel:
		op = OpDel
		doc, err = decodeKeyOnly(docBytes)
	default:
		err = fmt.Errorf("codec: unknown wal op %d", opByte)
	}
	if err != nil {
		return 0, 0, nil, 0, err
	}
	n = len(start) - (len(payload) - payloadLen)
	return seq, op, doc, n, nil
}

// ---- 快照记录与尾部 ----

// snapFooterLen 是快照尾部的固定字节数：docs(8) + maxSeq(8) + crc(4)。
const snapFooterLen = 8 + 8 + 4

// appendSnapRecord 追加一条快照记录：[seq][docLen][doc]（只含 OpPut 的完整 Doc）。
func appendSnapRecord(buf []byte, seq uint64, d *Doc) []byte {
	docBytes := encodeDoc(d)
	buf = appendUvarint(buf, seq)
	buf = appendUvarint(buf, uint64(len(docBytes)))
	buf = append(buf, docBytes...)
	return buf
}

// decodeSnapRecord 解码一条快照记录，返回 seq、doc 与剩余字节。
func decodeSnapRecord(data []byte) (seq uint64, doc *Doc, rest []byte, err error) {
	if seq, data, err = consumeUvarint(data); err != nil {
		return 0, nil, data, err
	}
	var docLen uint64
	if docLen, data, err = consumeUvarint(data); err != nil {
		return 0, nil, data, err
	}
	if docLen > uint64(len(data)) {
		return 0, nil, data, fmt.Errorf("codec: snap doc len %d exceeds %d", docLen, len(data))
	}
	doc, err = decodeDoc(data[:docLen])
	if err != nil {
		return 0, nil, data, err
	}
	return seq, doc, data[docLen:], nil
}

// appendSnapFooter 追加快照尾部：[docs u64 LE][maxSeq u64 LE][crc32 覆盖前述全部字节]。
func appendSnapFooter(buf []byte, docs, maxSeq uint64) []byte {
	var footer [snapFooterLen]byte
	binary.LittleEndian.PutUint64(footer[0:8], docs)
	binary.LittleEndian.PutUint64(footer[8:16], maxSeq)
	crc := crc32.Checksum(buf, castagnoli)
	binary.LittleEndian.PutUint32(footer[16:20], crc)
	return append(buf, footer[:]...)
}

// verifySnapFooter 校验快照尾部并返回 docs/maxSeq 与记录区字节。
// 返回的 records 是去掉尾部后的记录字节流。
func verifySnapFooter(data []byte) (records []byte, docs, maxSeq uint64, err error) {
	if len(data) < snapFooterLen {
		return nil, 0, 0, errors.New("codec: snapshot shorter than footer")
	}
	footer := data[len(data)-snapFooterLen:]
	records = data[:len(data)-snapFooterLen]
	docs = binary.LittleEndian.Uint64(footer[0:8])
	maxSeq = binary.LittleEndian.Uint64(footer[8:16])
	wantCrc := binary.LittleEndian.Uint32(footer[16:20])
	if crc32.Checksum(records, castagnoli) != wantCrc {
		return nil, 0, 0, fmt.Errorf("%w: snapshot", ErrCRC)
	}
	return records, docs, maxSeq, nil
}
