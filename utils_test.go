package imapsql

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"

	backendtests "github.com/foxcpp/go-imap-backend-tests"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

var TestDB string
var TestDSN string
var TestFSStore string
var TestPreserveDB bool

func TestMain(m *testing.M) {
	flag.StringVar(&TestDB, "test.driver", os.Getenv("TEST_DB"), "Database driver to use for tests by default")
	flag.StringVar(&TestDSN, "test.dsn", os.Getenv("TEST_DSN"), "DSN to use for tests by default")
	flag.StringVar(&TestFSStore, "test.fsstore", os.Getenv("TEST_FS_STORE"), "FS store root to use")
	flag.BoolVar(&TestPreserveDB, "test.preserve-db", os.Getenv("PRESERVE_DB") == "1", "Use persistent directory for SQLite DB and messages store")

	// go-imap-backend-tests assumes STATUS APPENDLIMIT reports only the
	// mailbox-specific value, with zero meaning "no limit". RFC 7889 defines
	// the item as the effective limit for the mailbox, including user and
	// backend-wide limits. The corrected behavior is covered by
	// TestStatusAppendLimitReflectsEffectiveLimit.
	backendtests.Blacklist = append(backendtests.Blacklist,
		"TestWithLZ4/Mailbox_AppendLimit/Status_-_No_Limit",
		"TestWithFSStore/Mailbox_AppendLimit/Status_-_No_Limit",
		"TestWithPostgres/Mailbox_AppendLimit/Status_-_No_Limit",
	)

	m.Run()
}

type testLogger struct {
	t *testing.T
}

func (t testLogger) Printf(format string, v ...any) {
	t.t.Logf(format, v...)
}

func (t testLogger) Println(v ...any) {
	t.t.Log(v...)
}

func (t testLogger) Debugf(format string, v ...any) {
	t.t.Logf(format, v...)
}

func (t testLogger) Debugln(v ...any) {
	t.t.Log(v...)
}

type testStoreObject struct {
	mu   sync.RWMutex
	data *bytes.Buffer
}

func (t *testStoreObject) Sync() error {
	return nil
}

func (t *testStoreObject) Read(p []byte) (n int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.data.Read(p)
}

func (t *testStoreObject) Write(p []byte) (n int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.data.Write(p)
}

func (t *testStoreObject) Close() error {
	return nil
}

type testStore struct {
	mu      sync.RWMutex
	objects map[string]*testStoreObject
}

func (t *testStore) Create(key string, objectSize int64) (ExtStoreObj, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	obj := &testStoreObject{}
	obj.data = bytes.NewBuffer(make([]byte, 0, objectSize))
	if t.objects == nil {
		t.objects = make(map[string]*testStoreObject)
	}
	t.objects[key] = obj
	return obj, nil
}

func (t *testStore) Open(key string) (ExtStoreObj, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	obj, ok := t.objects[key]
	if ok {
		// turn into handle independent of original object
		return &testStoreObject{
			data: bytes.NewBuffer(append([]byte{}, obj.data.Bytes()...)),
		}, nil
	}
	return nil, ExternalError{NonExistent: true, Key: key}
}

func (t *testStore) Delete(keys []string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, key := range keys {
		delete(t.objects, key)
	}
	return nil
}

func (t *testStore) DeleteAll() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.objects = make(map[string]*testStoreObject)
	return
}

func (t *testStore) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.objects)
}

func (t *testStore) Keys() []string {
	return slices.Collect(maps.Keys(t.objects))
}

type testBackendContainer struct {
	*Backend

	T *testing.T

	Opts *Opts

	Store ExternalStore

	Driver string
	DSN    string

	userCnt int
}

func (c *testBackendContainer) user() *User {
	userName := "testuser-" + strconv.Itoa(c.userCnt)

	err := c.Backend.CreateUser(userName)
	assert.NilError(c.T, err)

	c.userCnt++

	usr, err := c.Backend.GetUser(userName)
	assert.NilError(c.T, err)

	return usr.(*User)
}

func (c *testBackendContainer) mailbox(usr *User, name string) {
	assert.NilError(c.T, usr.CreateMailbox(name))
}

func wipeTestDB(t *testing.T, be *Backend) {
	if os.Getenv("PRESERVE_DB") == "1" || os.Getenv("PRESERVE_SQLITE3_DB") != "1" {
		return
	}

	// Remove things manually in the right order so we will not hit
	// foreign key constraint when dropping tables.
	if _, err := be.DB.Exec(`DELETE FROM msgs`); err != nil {
		t.Log("DELETE FROM msgs", err)
	}
	if _, err := be.DB.Exec(`DELETE FROM extKeys`); err != nil {
		t.Log("DELETE FROM extKeys", err)
	}

	if _, err := be.DB.Exec(`DROP TABLE flags`); err != nil {
		t.Log("DROP TABLE flags", err)
	}
	if _, err := be.DB.Exec(`DROP TABLE msgs`); err != nil {
		t.Log("DROP TABLE msgs", err)
	}
	if _, err := be.DB.Exec(`DROP TABLE mboxes`); err != nil {
		t.Log("DROP TABLE mboxes", err)
	}
	if _, err := be.DB.Exec(`DROP TABLE users`); err != nil {
		t.Log("DROP TABLE users", err)
	}
	if _, err := be.DB.Exec(`DROP TABLE extKeys`); err != nil {
		t.Log("DROP TABLE extKeys", err)
	}
}

func setupTestBackend(t *testing.T, opts ...func(*testBackendContainer)) *testBackendContainer {
	t.Helper()

	c := testBackendContainer{
		T:    t,
		Opts: &Opts{},
	}
	for _, opt := range opts {
		opt(&c)
	}

	if TestPreserveDB && c.DSN == "" && c.Store == nil {
		persistentTmpDir, err := os.MkdirTemp("", "go-imap-sql-tests-")
		assert.NilError(c.T, err)

		t.Log("Using file-backed sqlite3 DB in directory:", persistentTmpDir)

		c.Driver = "sqlite3"
		c.DSN = filepath.Join(persistentTmpDir, "test.db")

		assert.NilError(t, os.MkdirAll(filepath.Join(persistentTmpDir, "messages"), 0755))
		c.Store = &FSStore{Root: filepath.Join(persistentTmpDir, "messages")}
	}

	if c.Driver == "" {
		if TestDB == "" {
			c.Driver = "sqlite3"
		} else {
			c.Driver = TestDB
		}
	}
	if c.DSN == "" {
		if TestDSN != "" {
			c.DSN = TestDSN
		} else {
			c.DSN = TestDSN
		}
	}
	if c.Opts.PRNG == nil {
		randSrc := rand.NewSource(0)
		c.Opts.PRNG = rand.New(randSrc)
	}
	if c.Store == nil {
		c.Store = &testStore{}
	}
	if c.Opts.Log == nil {
		c.Opts.Log = testLogger{t: t}
	}

	b, err := New(c.Driver, c.DSN, c.Store, *c.Opts)
	assert.NilError(t, err)
	c.Backend = b

	t.Cleanup(func() { b.Close() })

	return &c
}

type syncFailStore struct{ ExternalStore }

type syncFailObject struct{ ExtStoreObj }

type retryDeleteStore struct {
	syncFailStore
	failed bool
}

func (s *retryDeleteStore) Delete(keys []string) error {
	if !s.failed {
		s.failed = true
		return errors.New("temporary delete failure")
	}
	return s.ExternalStore.Delete(keys)
}

func (syncFailObject) Sync() error { return errors.New("sync failed") }

func (s syncFailStore) Create(key string, size int64) (ExtStoreObj, error) {
	obj, err := s.ExternalStore.Create(key, size)
	if err != nil {
		return nil, err
	}
	return syncFailObject{obj}, nil
}

type closeFailCompression struct{}
type closeFailWriter struct{ io.Writer }

func (closeFailWriter) Close() error { return errors.New("compression flush failed") }
func (closeFailCompression) WrapCompress(w io.Writer, _ string) (io.WriteCloser, error) {
	return closeFailWriter{w}, nil
}
func (closeFailCompression) WrapDecompress(r io.Reader) (io.Reader, error) { return r, nil }

type brokenBuffer struct{}

func (brokenBuffer) Open() (io.ReadCloser, error) { return nil, errors.New("open failed") }

func emptyExtStore(s ExternalStore) is.Comparison {
	switch s := s.(type) {
	case *testStore:
		return is.Len(s.Keys(), 0)
	case *syncFailStore:
		return emptyExtStore(s.ExternalStore)
	case syncFailStore:
		return emptyExtStore(s.ExternalStore)
	case *retryDeleteStore:
		return emptyExtStore(s.ExternalStore)
	case *FSStore:
		return func() is.Result {
			entries, err := os.ReadDir(s.Root)
			if err != nil {
				return is.ResultFromError(err)
			}
			if len(entries) != 0 {
				return is.ResultFailure("entries in external store exist, expected empty")
			}
			return is.ResultSuccess
		}
	default:
		panic("unknown external store type")
	}
}
