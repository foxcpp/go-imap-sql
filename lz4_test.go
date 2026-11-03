package imapsql

import (
	"testing"

	backendtests "github.com/foxcpp/go-imap-backend-tests"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func TestWithLZ4(t *testing.T) {
	backendtests.RunTests(t, func() backendtests.Backend {
		be := setupTestBackend(t, func(c *testBackendContainer) {
			c.Opts.CompressAlgo = "lz4"
		})
		return be.Backend
	}, func(bi backendtests.Backend) {
		be := bi.(*Backend)

		wipeTestDB(t, be)

		be.extStore.(*testStore).DeleteAll()
	})
}
