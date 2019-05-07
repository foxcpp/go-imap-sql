package imapsql

import (
	"crypto/rand"
	"encoding/hex"
	"io"
)

/*
ExternalStore is an interface used by go-imap-sql to store message bodies
outside of main database.
*/
type ExternalStore interface {
	Create(key string, r io.Reader) error
	Open(key string) (io.ReadCloser, error)
	Delete(keys []string) error
}

func randomKey() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
