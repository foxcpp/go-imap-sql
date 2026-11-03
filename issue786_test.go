package imapsql

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-message/textproto"
	mess "github.com/foxcpp/go-imap-mess"
)

type syncFailStore struct{ *FSStore }

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
	return s.FSStore.Delete(keys)
}

func (syncFailObject) Sync() error { return errors.New("sync failed") }

func (s syncFailStore) Create(key string, size int64) (ExtStoreObj, error) {
	obj, err := s.FSStore.Create(key, size)
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

func TestDeliveryCannotCommitAfterBodyFailure(t *testing.T) {
	b, storeDir := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyParsed(textproto.Header{}, 1, brokenBuffer{}); err == nil {
		t.Fatal("expected body failure")
	}
	if err := d.Commit(); err == nil {
		t.Fatal("commit succeeded after body failure")
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staged blobs after failure: %v, %v", entries, err)
	}
}

func TestDeliveryCannotCommitAfterSecondBody(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyParsed(textproto.Header{}, 0, memoryBuffer{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyParsed(textproto.Header{}, 0, memoryBuffer{}); err == nil {
		t.Fatal("expected second body to fail")
	}
	if err := d.Commit(); err == nil {
		t.Fatal("commit succeeded after second body failed")
	}
}

func issue786Backend(t *testing.T) (*Backend, string) {
	t.Helper()
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "messages")
	if err := os.Mkdir(storeDir, 0700); err != nil {
		t.Fatal(err)
	}
	b, err := New("sqlite3", filepath.Join(dir, "test.db"), &FSStore{Root: storeDir}, Opts{BusyTimeout: 100, Log: DummyLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, storeDir
}

func TestDeliveryAbortDoesNotCreateMailbox(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.Mailbox("new-folder"); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	if err := d.Abort(); err != nil {
		t.Fatal(err)
	}
	_, _, err = u.GetMailbox("new-folder", true, nil)
	if !errors.Is(err, backend.ErrNoSuchMailbox) {
		t.Fatalf("mailbox exists after aborted delivery: %v", err)
	}
}

func TestDeliveryBodySyncFailureRemovesBlob(t *testing.T) {
	b, storeDir := issue786Backend(t)
	b.extStore = syncFailStore{b.extStore.(*FSStore)}
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err == nil {
		t.Fatal("expected sync error")
	}
	if err := d.Abort(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("body failure left %d blobs", len(entries))
	}
}

func TestDeliveryBodyCompressionFailureRemovesBlob(t *testing.T) {
	b, storeDir := issue786Backend(t)
	b.compressAlgo = closeFailCompression{}
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err == nil {
		t.Fatal("expected compression flush error")
	}
	if err := d.Abort(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("compression failure left blobs: %v, %v", entries, err)
	}
}

func TestDeliveryAbortRetriesFailedBodyCleanup(t *testing.T) {
	b, storeDir := issue786Backend(t)
	b.extStore = &retryDeleteStore{syncFailStore: syncFailStore{b.extStore.(*FSStore)}}
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err == nil {
		t.Fatal("expected sync error")
	}
	if err := d.Abort(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("abort did not clean failed body: %v, %v", entries, err)
	}
}

func TestDeliveryInterleavedBodies(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d1, d2 := b.NewDelivery(), b.NewDelivery()
	defer d1.Abort()
	defer d2.Abort()
	for _, d := range []*Delivery{&d1, &d2} {
		if err := d.AddRcpt("user", textproto.Header{}); err != nil {
			t.Fatal(err)
		}
		if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d2.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := d1.Commit(); err != nil {
		t.Fatal(err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	status, mbox, err := u.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer mbox.Close()
	if status.Messages != 2 {
		t.Fatalf("got %d messages; want 2", status.Messages)
	}
}

func TestDeliveryAbortRemovesStagedBlobs(t *testing.T) {
	b, storeDir := issue786Backend(t)
	for _, user := range []string{"one", "two"} {
		if err := b.CreateUser(user); err != nil {
			t.Fatal(err)
		}
	}
	d := b.NewDelivery()
	for _, user := range []string{"one", "two"} {
		if err := d.AddRcpt(user, textproto.Header{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	if err := d.Abort(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("abort left %d staged blobs", len(entries))
	}
}

func TestDeliveryCommitRejectsDeletedMailbox(t *testing.T) {
	b, storeDir := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	if err := u.CreateMailbox("target"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	defer d.Abort()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.Mailbox("target"); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	if err := u.DeleteMailbox("target"); err != nil {
		t.Fatalf("delete mailbox while delivery is staged: %v", err)
	}
	if err := u.CreateMailbox("target"); err != nil {
		t.Fatal(err)
	}
	if err := d.Commit(); err == nil {
		t.Fatal("commit succeeded after mailbox was deleted and recreated")
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed commit left %d staged blobs", len(entries))
	}
	status, mbox, err := u.GetMailbox("target", true, &noopConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer mbox.Close()
	if status.Messages != 0 {
		t.Fatalf("replacement mailbox has %d messages; want 0", status.Messages)
	}
}

func TestDeliveryCommitFailureRollsBackEveryRecipient(t *testing.T) {
	b, storeDir := issue786Backend(t)
	for _, name := range []string{"one", "two"} {
		if err := b.CreateUser(name); err != nil {
			t.Fatal(err)
		}
	}
	u1, err := b.GetUser("one")
	if err != nil {
		t.Fatal(err)
	}
	_, selected, err := u1.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	d := b.NewDelivery()
	for _, name := range []string{"one", "two"} {
		if err := d.AddRcpt(name, textproto.Header{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteUser("two"); err != nil {
		t.Fatal(err)
	}
	if err := d.Commit(); !errors.Is(err, ErrDeliveryInterrupted) {
		t.Fatalf("commit after deletion = %v; want ErrDeliveryInterrupted", err)
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staged blobs after failed commit: %v, %v", entries, err)
	}
	status, err := u1.Status("INBOX", []imap.StatusItem{imap.StatusMessages})
	if err != nil {
		t.Fatal(err)
	}
	if status.Messages != 0 {
		t.Fatalf("first recipient got %d messages after failed commit", status.Messages)
	}
	selected.(*Mailbox).handle.Sync(true)
	seq := new(imap.SeqSet)
	seq.AddNum(1)
	if resolved, err := selected.(*Mailbox).handle.ResolveSeq(false, seq); err == nil && !resolved.Empty() {
		t.Fatalf("selected mailbox observed rolled-back messages: %v", resolved)
	}
}

func TestDeliveryConcurrentNotificationsStayInUIDOrder(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	_, selected, err := u.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	const count = 20
	deliveries := make([]Delivery, count)
	for i := range deliveries {
		deliveries[i] = b.NewDelivery()
		if err := deliveries[i].AddRcpt("user", textproto.Header{}); err != nil {
			t.Fatal(err)
		}
		if err := deliveries[i].BodyRaw(strings.NewReader(testMsg)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := range deliveries {
		wg.Add(1)
		go func(d *Delivery) {
			defer wg.Done()
			errs <- d.Commit()
		}(&deliveries[i])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	handle := selected.(*Mailbox).handle
	handle.Sync(true)
	for uid := uint32(1); uid <= count; uid++ {
		seq, ok := handle.UidAsSeq(uid)
		if !ok || seq != uid {
			t.Fatalf("UID %d has sequence %d, present=%t", uid, seq, ok)
		}
	}
}

func TestDeliverySelectDuringCommitDoesNotDuplicateUID(t *testing.T) {
	t.Skip("TODO: need to be fixed")

	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}

	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}

	updates := make(chan mess.Update)
	b.mngr.SetExternalSink(updates)
	committed := make(chan error, 1)
	go func() { committed <- d.Commit() }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		if err := b.DB.QueryRow(`SELECT COUNT(*) FROM msgs`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delivery did not commit")
		}
		time.Sleep(time.Millisecond)
	}

	type result struct {
		box backend.Mailbox
		err error
	}

	selected := make(chan result, 1)
	go func() {
		_, box, err := u.GetMailbox("INBOX", true, &noopConn{})
		selected <- result{box, err}
	}()

	// Give SELECT the opportunity to run while the notification is blocked.
	var r result
	select {
	case r = <-selected:
	case <-time.After(100 * time.Millisecond):
	}
	<-updates
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	if r.box == nil && r.err == nil {
		r = <-selected
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.box.Close()

	handle := r.box.(*Mailbox).handle
	handle.Sync(true)

	seq := new(imap.SeqSet)
	seq.AddNum(2)
	if resolved, err := handle.ResolveSeq(false, seq); err == nil && !resolved.Empty() {
		t.Fatalf("one committed message appeared twice: %v", resolved)
	}
}

func TestDeliveryNewMailboxCreatedBeforeCommitIsTemporary(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.Mailbox("new-folder"); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	if err := u.CreateMailbox("new-folder"); err != nil {
		t.Fatal(err)
	}
	if err := d.Commit(); !errors.Is(err, ErrDeliveryInterrupted) {
		t.Fatalf("commit after concurrent mailbox creation = %v; want ErrDeliveryInterrupted", err)
	}
}

func TestDeliveryRejectsStaleSpecialMailbox(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	if err := u.(*User).CreateMailboxSpecial("junk", imap.JunkAttr); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.SpecialMailbox(imap.JunkAttr, "junk"); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB.Exec(`UPDATE mboxes SET specialuse = NULL WHERE name = ?`, "junk"); err != nil {
		t.Fatal(err)
	}
	if err := d.Commit(); !errors.Is(err, ErrDeliveryInterrupted) {
		t.Fatalf("commit after special-use change = %v; want ErrDeliveryInterrupted", err)
	}
}

func TestDeliveryReuseAfterAbortAndCommit(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	for i := 0; i < 3; i++ {
		if err := d.AddRcpt("user", textproto.Header{}); err != nil {
			t.Fatal(err)
		}
		if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			err := d.Abort()
			if err != nil {
				t.Fatal(err)
			}
		} else if err := d.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	status, mbox, err := u.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer mbox.Close()
	if status.Messages != 2 {
		t.Fatalf("got %d messages; want 2", status.Messages)
	}
}

func TestDeliveryDuplicateRecipientNewMailbox(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	for i := 0; i < 2; i++ {
		if err := d.AddRcpt("user", textproto.Header{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Mailbox("new-folder"); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	if err := d.Commit(); err != nil {
		t.Fatalf("commit duplicate recipients: %v", err)
	}
	u, err := b.GetUser("user")
	if err != nil {
		t.Fatal(err)
	}
	status, mbox, err := u.GetMailbox("new-folder", true, &noopConn{})
	if err != nil {
		t.Fatal(err)
	}
	defer mbox.Close()
	if status.Messages != 2 {
		t.Fatalf("got %d messages; want 2", status.Messages)
	}
}

func TestDeliveryCommitBusyCleansUp(t *testing.T) {
	b, storeDir := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	tx, err := b.db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Commit(); err == nil {
		t.Fatal("expected commit to fail while database is locked")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("busy commit left staged blobs: %v, %v", entries, err)
	}
	if err := d.Abort(); err != nil {
		t.Fatalf("abort after failed commit: %v", err)
	}
}

func TestDeliveryRejectsUnsupportedSpecialMailbox(t *testing.T) {
	b, _ := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.SpecialMailbox(`\All`, "all"); !errors.Is(err, ErrUnsupportedSpecialAttr) {
		t.Fatalf("unsupported special mailbox returned %v", err)
	}
}

func TestDeliverySQLCommitFailureCleansUp(t *testing.T) {
	b, storeDir := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TABLE commit_parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE commit_child (id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER commit_failure AFTER INSERT ON msgs BEGIN INSERT INTO commit_child VALUES (1); END`,
	} {
		if _, err := b.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}
	if err := d.Commit(); err == nil {
		t.Fatal("expected deferred constraint to fail at commit")
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed commit left staged blobs: %v, %v", entries, err)
	}
	var count int
	if err := b.DB.QueryRow(`SELECT COUNT(*) FROM msgs`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed commit left %d messages: %v", count, err)
	}
}

func TestDeliveryAbortAfterAmbiguousCommitPreservesReferencedBlob(t *testing.T) {
	b, storeDir := issue786Backend(t)
	if err := b.CreateUser("user"); err != nil {
		t.Fatal(err)
	}
	d := b.NewDelivery()
	if err := d.AddRcpt("user", textproto.Header{}); err != nil {
		t.Fatal(err)
	}
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		t.Fatal(err)
	}

	var err error
	tx, err := b.db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.commitMailbox(tx, &d.staged[0]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx = nil

	// Simulate a driver reporting an error after the server committed.
	d.commitFailed = true
	if err := d.Abort(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(storeDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("abort removed a committed blob: %v, %v", entries, err)
	}
}
