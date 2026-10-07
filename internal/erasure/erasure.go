package erasure

import (
	"bytes"

	"github.com/klauspost/reedsolomon"
)

type Encoder struct {
	dataShards   int
	parityShards int
	enc          reedsolomon.Encoder
}

// New 创建一个纠删码编解码器。底层使用 Reed-Solomon，要求 dataShards >= 1 且 parityShards >= 1。
func New(dataShards, parityShards int) (*Encoder, error) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	return &Encoder{dataShards: dataShards, parityShards: parityShards, enc: enc}, nil
}

// Shards 返回总分片数（数据分片 + 校验分片）。
func (e *Encoder) Shards() int {
	return e.dataShards + e.parityShards
}

// DataShards 返回数据分片数。
func (e *Encoder) DataShards() int {
	return e.dataShards
}

// ParityShards 返回校验分片数。
func (e *Encoder) ParityShards() int {
	return e.parityShards
}

// Encode 把数据编码为分片切片：先 Split 切成 dataShards 份等长数据分片，
// 再由 Encode 计算 parityShards 份校验分片，返回长度为 Shards() 的切片。
func (e *Encoder) Encode(data []byte) ([][]byte, error) {
	shards, err := e.enc.Split(data)
	if err != nil {
		return nil, err
	}
	if err := e.enc.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

// Decode 从可能缺失的分片中恢复数据：Reconstruct 先补齐缺失分片，
// 再 Join 合并数据分片并按 size 截断，得到原始字节。
func (e *Encoder) Decode(shards [][]byte, size int) ([]byte, error) {
	if err := e.enc.Reconstruct(shards); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := e.enc.Join(&buf, shards, size); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
