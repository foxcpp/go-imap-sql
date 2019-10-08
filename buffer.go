package imapsql

import (
	"bytes"
	"io"
)

type byter interface {
	Bytes() []byte
}

// bufferBody does work necessary to get all contents of the io.Reader
// (until EOF) into a single []byte. It takes shortcuts if io.Reader
// is already a wrapper over a []byte.
// Returned slice should not be mutated.
func bufferBody(r io.Reader) ([]byte, error) {
	b, ok := r.(byter)
	if ok {
		return b.Bytes(), nil
	}

	var buf bytes.Buffer
	// 125 KiB, empirically observed max size for a typical message.
	buf.Grow(125 * 1024)

	_, err := buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
