package catalog

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func testDoc() *Doc {
	return &Doc{
		Bucket:       "photos",
		Key:          "2026/11/beach.jpg",
		VersionID:    "v1234567890",
		IsLatest:     true,
		DeleteMarker: false,
		Size:         1048576,
		ETag:         "d41d8cd98f00b204e9800998ecf8427e",
		ContentType:  "image/jpeg",
		ModTime:      time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC),
		UserMeta:     map[string]string{"x-amz-meta-owner": "alice", "x-amz-meta-project": "p1"},
		Tags:         map[string]string{"env": "prod", "team": "data"},
		Seq:          42,
	}
}

func equalDoc(a, b *Doc) bool {
	if a.Bucket != b.Bucket || a.Key != b.Key || a.VersionID != b.VersionID ||
		a.IsLatest != b.IsLatest || a.DeleteMarker != b.DeleteMarker ||
		a.Size != b.Size || a.ETag != b.ETag || a.ContentType != b.ContentType ||
		!a.ModTime.Equal(b.ModTime) {
		return false
	}
	return equalStrMap(a.UserMeta, b.UserMeta) && equalStrMap(a.Tags, b.Tags)
}

func equalStrMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestDocRoundtrip(t *testing.T) {
	orig := testDoc()
	got, err := decodeDoc(encodeDoc(orig))
	if err != nil {
		t.Fatalf("decodeDoc: %v", err)
	}
	if !equalDoc(orig, got) {
		t.Fatalf("roundtrip mismatch:\n orig=%+v\n got =%+v", orig, got)
	}
}

func TestDocRoundtripEmpty(t *testing.T) {
	// 空 map / 零值也应能 roundtrip（注意：Seq 不参与编码）
	orig := &Doc{Bucket: "b", Key: "k", VersionID: "null"}
	got, err := decodeDoc(encodeDoc(orig))
	if err != nil {
		t.Fatalf("decodeDoc: %v", err)
	}
	if got.Bucket != "b" || got.Key != "k" || got.VersionID != "null" {
		t.Fatalf("empty roundtrip mismatch: %+v", got)
	}
	if got.IsLatest || got.DeleteMarker || got.Size != 0 || !got.ModTime.IsZero() {
		t.Fatalf("empty roundtrip unexpected fields: %+v", got)
	}
}

func TestDocKeyOnlyRoundtrip(t *testing.T) {
	orig := &Doc{Bucket: "b", Key: "k/v", VersionID: "v9"}
	got, err := decodeKeyOnly(encodeKeyOnly(orig))
	if err != nil {
		t.Fatalf("decodeKeyOnly: %v", err)
	}
	if got.Bucket != "b" || got.Key != "k/v" || got.VersionID != "v9" {
		t.Fatalf("key-only roundtrip mismatch: %+v", got)
	}
}

func TestWALRecordRoundtrip(t *testing.T) {
	for _, op := range []Op{OpPut, OpDel} {
		d := testDoc()
		record := appendWALRecord(nil, 7, op, d)
		seq, gotOp, got, n, err := decodeWALRecord(record)
		if err != nil {
			t.Fatalf("op=%v decodeWALRecord: %v", op, err)
		}
		if seq != 7 || gotOp != op {
			t.Fatalf("op=%v seq/op mismatch: seq=%d op=%v", op, seq, gotOp)
		}
		if n != len(record) {
			t.Fatalf("op=%v consumed %d, want %d", op, n, len(record))
		}
		if op == OpPut {
			if !equalDoc(d, got) {
				t.Fatalf("op=put roundtrip mismatch: %+v vs %+v", d, got)
			}
		} else if got.Bucket != d.Bucket || got.Key != d.Key || got.VersionID != d.VersionID {
			t.Fatalf("op=del key-only mismatch: %+v", got)
		}
	}
}

func TestWALRecordCRCDetect(t *testing.T) {
	record := appendWALRecord(nil, 1, OpPut, testDoc())
	// 翻转最后一个字节（位于 doc payload 内），应报 crc mismatch
	bad := append([]byte(nil), record...)
	bad[len(bad)-1] ^= 0xFF
	if _, _, _, _, err := decodeWALRecord(bad); err == nil || !strings.Contains(err.Error(), "crc") {
		t.Fatalf("expected crc error, got %v", err)
	}
}

func TestSnapRecordRoundtrip(t *testing.T) {
	d := testDoc()
	data := appendSnapRecord(nil, 99, d)
	seq, got, rest, err := decodeSnapRecord(data)
	if err != nil {
		t.Fatalf("decodeSnapRecord: %v", err)
	}
	if seq != 99 || len(rest) != 0 {
		t.Fatalf("seq=%d rest=%d", seq, len(rest))
	}
	if !equalDoc(d, got) {
		t.Fatalf("snap roundtrip mismatch: %+v vs %+v", d, got)
	}
}

func TestSnapFooterRoundtrip(t *testing.T) {
	var buf []byte
	for i, d := range []*Doc{testDoc(), {Bucket: "b", Key: "k2", VersionID: "v"}} {
		buf = appendSnapRecord(buf, uint64(i+1), d)
	}
	buf = appendSnapFooter(buf, 2, 100)
	records, docs, maxSeq, err := verifySnapFooter(buf)
	if err != nil {
		t.Fatalf("verifySnapFooter: %v", err)
	}
	if docs != 2 || maxSeq != 100 {
		t.Fatalf("docs=%d maxSeq=%d", docs, maxSeq)
	}
	if !bytes.Equal(records, buf[:len(buf)-snapFooterLen]) {
		t.Fatal("records region mismatch")
	}
}

func TestSnapFooterCRCDetect(t *testing.T) {
	var buf []byte
	buf = appendSnapRecord(buf, 1, testDoc())
	buf = appendSnapFooter(buf, 1, 1)
	bad := append([]byte(nil), buf...)
	bad[0] ^= 0xFF // 翻转记录区第一个字节
	if _, _, _, err := verifySnapFooter(bad); err == nil || !strings.Contains(err.Error(), "crc") {
		t.Fatalf("expected crc error, got %v", err)
	}
}

func TestWALRecordStream(t *testing.T) {
	// 多条记录拼成一个流，逐条解码，验证边界与 crc 正确
	var buf []byte
	for i := 0; i < 50; i++ {
		d := testDoc()
		d.Size = int64(i)
		buf = appendWALRecord(buf, uint64(i+1), OpPut, d)
	}
	rest := buf
	for i := 0; i < 50; i++ {
		seq, op, doc, n, err := decodeWALRecord(rest)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if seq != uint64(i+1) || op != OpPut || doc.Size != int64(i) {
			t.Fatalf("record %d mismatch: seq=%d size=%d", i, seq, doc.Size)
		}
		rest = rest[n:]
	}
	if len(rest) != 0 {
		t.Fatalf("leftover %d bytes after stream", len(rest))
	}
}

func TestVarintEdge(t *testing.T) {
	for _, v := range []int64{0, -1, 1, 1 << 40, -(1 << 40), int64(^uint64(0) >> 1)} {
		got, _, err := consumeVarint(appendVarint(nil, v))
		if err != nil {
			t.Fatalf("varint %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("varint roundtrip: %d != %d", got, v)
		}
	}
}
