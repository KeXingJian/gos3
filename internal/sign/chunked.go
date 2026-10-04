package sign

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"strconv"
	"strings"
)

type chunkedReader struct {
	br         *bufio.Reader
	closer     io.Closer
	signingKey []byte
	scope      string
	amzDate    string
	prevSig    string
	buf        []byte
	done       bool
}

func NewChunkedReader(r io.Reader, signingKey []byte, scope, amzDate, seedSig string) io.ReadCloser {
	c := &chunkedReader{
		br:         bufio.NewReader(r),
		signingKey: signingKey,
		scope:      scope,
		amzDate:    amzDate,
		prevSig:    seedSig,
	}
	if closer, ok := r.(io.Closer); ok {
		c.closer = closer
	}
	return c
}

func (c *chunkedReader) Close() error {
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(c.buf) == 0 {
		if c.done {
			return 0, io.EOF
		}
		if err := c.nextChunk(); err != nil {
			return 0, err
		}
		if len(c.buf) == 0 {
			return 0, io.EOF
		}
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *chunkedReader) nextChunk() error {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimRight(line, "\r\n")
	head, tail, ok := strings.Cut(line, ";")
	if !ok {
		return ErrInvalidAuth
	}
	size, err := strconv.ParseInt(strings.TrimSpace(head), 16, 64)
	if err != nil || size < 0 {
		return ErrInvalidAuth
	}
	signature := ""
	for _, attr := range strings.Split(tail, ";") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(attr), "chunk-signature="); ok {
			signature = v
		}
	}
	if signature == "" {
		return ErrInvalidAuth
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(c.br, data); err != nil {
		return err
	}
	crlf := make([]byte, 2)
	if _, err := io.ReadFull(c.br, crlf); err != nil {
		return err
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return ErrInvalidAuth
	}

	chunkHash := sha256.Sum256(data)
	sts := Algorithm + "-PAYLOAD\n" + c.amzDate + "\n" + c.scope + "\n" +
		c.prevSig + "\n" + EmptySHA256 + "\n" + hex.EncodeToString(chunkHash[:])
	expected := hex.EncodeToString(hmacSHA256(c.signingKey, sts))
	if subtle.ConstantTimeCompare([]byte(expected), []byte(strings.ToLower(signature))) != 1 {
		return ErrSignature
	}
	c.prevSig = strings.ToLower(signature)

	if size == 0 {
		c.done = true
		return nil
	}
	c.buf = data
	return nil
}
