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

func New(dataShards, parityShards int) (*Encoder, error) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	return &Encoder{dataShards: dataShards, parityShards: parityShards, enc: enc}, nil
}

func (e *Encoder) Shards() int {
	return e.dataShards + e.parityShards
}

func (e *Encoder) DataShards() int {
	return e.dataShards
}

func (e *Encoder) ParityShards() int {
	return e.parityShards
}

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
