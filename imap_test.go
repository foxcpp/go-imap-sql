package imapsql

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/foxcpp/go-imap-backend-tests"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var TestDB = os.Getenv("TEST_DB")
var TestDSN = os.Getenv("TEST_DSN")

func initTestBackend() backendtests.Backend {
	driver := TestDB
	dsn := TestDSN

	if TestDB == "" {
		driver = "sqlite3"
		dsn = ":memory:"
	}

	randSrc := rand.NewSource(0)
	prng := rand.New(randSrc)

	// This is meant for DB debugging.
	if os.Getenv("PRESERVE_SQLITE3_DB") == "1" {
		log.Println("Using sqlite3 DB in temporary directory.")
		tempDir, err := ioutil.TempDir("", "go-imap-sql-tests-")
		if err != nil {
			panic(err)
		}
		driver = "sqlite3"
		dsn = filepath.Join(tempDir, "test.db")
	}

	b, err := New(driver, dsn, Opts{
		LazyUpdatesInit: true,
		PRNG:            prng,
	})
	if err != nil {
		panic(err)
	}
	return b
}

func cleanBackend(bi backendtests.Backend) {
	b := bi.(*Backend)
	if os.Getenv("PRESERVE_DB") != "1" {
		if _, err := b.db.Exec(`DROP TABLE flags`); err != nil {
			panic(err)
		}
		if _, err := b.db.Exec(`DROP TABLE msgs`); err != nil {
			panic(err)
		}
		if _, err := b.db.Exec(`DROP TABLE mboxes`); err != nil {
			panic(err)
		}
		if _, err := b.db.Exec(`DROP TABLE users`); err != nil {
			panic(err)
		}
	}
	b.Close()
}

func TestBackend(t *testing.T) {
	backendtests.Blacklist = []string{
		// FIXME: not handled correctly by backendutil, https://github.com/emersion/go-imap/pull/240
		"TestBackend/Mailbox_ListMessages_Body/BODY[HEADER.FIELDS.NOT_(From_To)]",
		"TestBackend/Mailbox_ListMessages_Body/BODY[1.1.HEADER.FIELDS_(Content-Type)]",
		"TestBackend/Mailbox_ListMessages_Body/BODY[1.1.HEADER.FIELDS.NOT_(Content-Type)]",

		// FIXME: not handled correctly by backendutil
		"TestBackend/Mailbox_SearchMessages/Crit_4",
	}
	backendtests.RunTests(t, initTestBackend, cleanBackend)
}
