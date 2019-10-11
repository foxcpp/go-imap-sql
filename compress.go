package imapsql

import (
	"io"
	"strconv"

	"github.com/pierrec/lz4"
)

type CompressionAlgo interface {
	// WrapCompress wraps writer such that any data written to it
	// will be compressed using a certain compression algorithms.
	//
	// Close on returned writer should not close original writer, but
	// should flush any buffers if necessary.
	//
	// Algorithm settings can be customized by passing
	// implementation-defined params argument. Most algorithms
	// will include compression level here as a string. More complex
	// algorithms can use JSON to store complex settings. Empty string
	// means that the default parameters should be used.
	WrapCompress(w io.Writer, params string) (io.WriteCloser, error)

	// WrapDecompress wraps writer such that underlying stream should be decompressed
	// using a certain compression algorithms.
	WrapDecompress(r io.Reader) (io.Reader, error)
}

var compressionAlgos = map[string]CompressionAlgo{
	"lz4": lz4Compression{},
}

// RegisterCompressionAlgo adds a new compression algorithm to the registry so it can
// be used in Opts.CompressionAlgo.
func RegisterCompressionAlgo(name string, algo CompressionAlgo) {
	compressionAlgos[name] = algo
}

type lz4Compression struct{}

func (algo lz4Compression) WrapCompress(w io.Writer, params string) (io.WriteCloser, error) {
	lz4w := lz4.NewWriter(w)
	if params != "" {
		var err error
		lz4w.CompressionLevel, err = strconv.Atoi(params)
		if err != nil {
			return nil, err
		}
	}
	return lz4w, nil
}

func (algo lz4Compression) WrapDecompress(r io.Reader) (io.Reader, error) {
	return lz4.NewReader(r), nil
}

type nullCompression struct{}

func (algo nullCompression) WrapCompress(w io.Writer, params string) (io.WriteCloser, error) {
	return nopCloser{w}, nil
}

func (algo nullCompression) WrapDecompress(r io.Reader) (io.Reader, error) {
	return r, nil
}
