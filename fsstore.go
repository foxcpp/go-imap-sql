package imapsql

import (
	"os"
	"path/filepath"
)

// FSStore struct represents directory on FS used to store message bodies.
//
// Always use field names on initialization because new fields may be added
// without a major version change.
type FSStore struct {
	Root string
}

func (s *FSStore) Open(key string) (ExtStoreObj, error) {
	return os.Open(filepath.Join(s.Root, key))
}

func (s *FSStore) Create(key string) (ExtStoreObj, error) {
	return os.Create(filepath.Join(s.Root, key))
}

func (s *FSStore) Delete(keys []string) error {
	for _, key := range keys {
		if err := os.Remove(filepath.Join(s.Root, key)); err != nil {
			return err
		}
	}
	return nil
}
