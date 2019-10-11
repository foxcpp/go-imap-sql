package imapsql

import (
	"io"
	"os"
	"path/filepath"
)

// Store struct represents directory on FS used to store message bodies.
//
// Always use field names on initialization because new fields may be added
// without a major version change.
type Store struct {
	Root string
}

func (s *Store) Open(key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.Root, key))
}

func (s *Store) Create(key string) (io.WriteCloser, error) {
	return os.Create(filepath.Join(s.Root, key))
}

func (s *Store) Delete(keys []string) error {
	for _, key := range keys {
		if err := os.Remove(filepath.Join(s.Root, key)); err != nil {
			return err
		}
	}
	return nil
}
