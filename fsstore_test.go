package imapsql

import (
	"os"
	"testing"

	backendtests "github.com/foxcpp/go-imap-backend-tests"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func TestWithFSStore(t *testing.T) {
	backendtests.RunTests(t, func() backendtests.Backend {
		be := setupTestBackend(t, func(c *testBackendContainer) {
			c.Store = &FSStore{Root: t.TempDir()}
		})
		return be.Backend
	}, func(bi backendtests.Backend) {
		b := bi.(*Backend)
		wipeTestDB(t, b)
		if err := os.RemoveAll(b.extStore.(*FSStore).Root); err != nil {
			t.Log(err)
		}
	})
}
