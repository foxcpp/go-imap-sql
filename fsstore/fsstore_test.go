package fsstore

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	backendtests "github.com/foxcpp/go-imap-backend-tests"
	imapsql "github.com/foxcpp/go-imap-sql"
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

	tempDir, err := ioutil.TempDir("", "go-imap-sql-tests-")
	if err != nil {
		panic(err)
	}

	// This is meant for DB debugging.
	if os.Getenv("PRESERVE_SQLITE3_DB") == "1" {
		log.Println("Using sqlite3 DB in temporary directory.")
		driver = "sqlite3"
		dsn = filepath.Join(tempDir, "test.db")
	}

	storeDir := filepath.Join(tempDir, "store")
	if err := os.MkdirAll(storeDir, os.ModeDir|os.ModePerm); err != nil {
		panic(err)
	}

	b, err := imapsql.New(driver, dsn, imapsql.Opts{
		LazyUpdatesInit: true,
		PRNG:            prng,
		ExternalStore:   &Store{Root: storeDir},
	})
	if err != nil {
		panic(err)
	}
	return b
}

func cleanBackend(bi backendtests.Backend) {
	b := bi.(*imapsql.Backend)
	if os.Getenv("PRESERVE_DB") != "1" {
		if _, err := b.DB.Exec(`DROP TABLE flags`); err != nil {
			log.Println("DROP TABLE flags", err)
		}
		if _, err := b.DB.Exec(`DROP TABLE msgs`); err != nil {
			log.Println("DROP TABLE msgs", err)
		}
		if _, err := b.DB.Exec(`DROP TABLE mboxes`); err != nil {
			log.Println("DROP TABLE mboxes", err)
		}
		if _, err := b.DB.Exec(`DROP TABLE users`); err != nil {
			log.Println("DROP TABLE users", err)
		}
		if _, err := b.DB.Exec(`DROP TABLE extKeys`); err != nil {
			log.Println("DROP TABLE users", err)
		}

		if err := os.RemoveAll(b.Opts.ExternalStore.(*Store).Root); err != nil {
			log.Println(err)
		}
	}
	b.Close()
}

func TestWithFSStore(t *testing.T) {
	backendtests.Blacklist = []string{
		// FIXME: not handled correctly by backendutil, https://github.com/emersion/go-imap/pull/240
		"TestWithFSStore/Mailbox_ListMessages_Body/BODY[HEADER.FIELDS.NOT_(From_To)]",
		"TestWithFSStore/Mailbox_ListMessages_Body/BODY[1.1.HEADER.FIELDS_(Content-Type)]",
		"TestWithFSStore/Mailbox_ListMessages_Body/BODY[1.1.HEADER.FIELDS.NOT_(Content-Type)]",

		// FIXME: not handled correctly by backendutil
		"TestWithFSStore/Mailbox_SearchMessages/Crit_4",
	}
	backendtests.RunTests(t, initTestBackend, cleanBackend)
}
