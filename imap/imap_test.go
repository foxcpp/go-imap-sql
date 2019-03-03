package imap

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/foxcpp/go-sqlmail/imap/testsuite"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var TestDB = os.Getenv("TEST_DB")
var TestDSN = os.Getenv("TEST_DSN")

func initTestBackend() testsuite.Backend {
	driver := TestDB
	dsn := TestDSN

	if TestDB == "" {
		driver = "sqlite3"
		dsn = ":memory:"
	}

	// This is meant for DB debugging.
	if os.Getenv("PRESERVE_SQLITE3_DB") == "1" {
		log.Println("Using sqlite3 DB in temporary directory.")
		tempDir, err := ioutil.TempDir("", "go-sqlmail-tests-")
		if err != nil {
			panic(err)
		}
		driver = "sqlite3"
		dsn = filepath.Join(tempDir, "test.db")
	}

	b, err := NewBackend(driver, dsn)
	if err != nil {
		panic(err)
	}
	return b
}

func cleanBackend(bi testsuite.Backend) {
	b := bi.(*Backend)
	b.db.Exec(`DROP TABLE users`)
	b.db.Exec(`DROP TABLE mboxes`)
	b.db.Exec(`DROP TABLE flags`)
	b.db.Exec(`DROP TABLE msgs`)
	b.Close()
}

func TestBackend(t *testing.T) {
	testsuite.RunTests(t, initTestBackend, cleanBackend)
}
