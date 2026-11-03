package imapsql

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-message/textproto"
	mess "github.com/foxcpp/go-imap-mess"
	"gotest.tools/assert"
)

func setupTestBackendWithFSStore(t *testing.T) (*testBackendContainer, string) {
	t.Helper()
	store := &FSStore{Root: t.TempDir()}
	b := setupTestBackend(t, func(c *testBackendContainer) {
		c.Store = store
	})
	return b, store.Root
}

func setupTestBackendForConcurrentSQLite(t *testing.T) *testBackendContainer {
	t.Helper()
	return setupTestBackend(t, func(c *testBackendContainer) {
		c.Driver = "sqlite3"
		c.DSN = filepath.Join(t.TempDir(), "imapsql.db")
	})
}

func TestDeliveryCannotCommitAfterBodyFailure(t *testing.T) {
	b := setupTestBackend(t)
	usr := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(usr.username, textproto.Header{}))
	assert.Assert(t, d.BodyParsed(textproto.Header{}, 1, brokenBuffer{}) != nil, "expected body failure")
	assert.Assert(t, d.Commit() != nil, "commit succeeded after body failure")

	assert.Assert(t, emptyExtStore(b.Store))
}

func TestDeliveryCannotCommitAfterSecondBody(t *testing.T) {
	b := setupTestBackend(t)
	usr := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(usr.username, textproto.Header{}))
	assert.NilError(t, d.BodyParsed(textproto.Header{}, 0, memoryBuffer{}))
	assert.Assert(t, d.BodyParsed(textproto.Header{}, 0, memoryBuffer{}) != nil, "expected second body to fail")
	assert.Assert(t, d.Commit() != nil, "commit succeeded after second body failed")
}

func TestDeliveryAbortDoesNotCreateMailbox(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.username, textproto.Header{}))
	assert.NilError(t, d.Mailbox("new-folder"))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))
	assert.NilError(t, d.Abort())

	_, _, err := u.GetMailbox("new-folder", true, nil)
	assert.Assert(t, errors.Is(err, backend.ErrNoSuchMailbox), "mailbox exists after aborted delivery: %v", err)
}

func TestDeliveryBodySyncFailureRemovesBlob(t *testing.T) {
	b := setupTestBackend(t, func(c *testBackendContainer) {
		c.Store = syncFailStore{ExternalStore: &testStore{}}
	})
	usr := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(usr.username, textproto.Header{}))
	assert.Assert(t, d.BodyRaw(strings.NewReader(testMsg)) != nil, "expected sync error")
	if err := d.Abort(); err != nil {
		assert.NilError(t, err)
	}

	assert.Assert(t, emptyExtStore(b.Store))
}

func TestDeliveryBodyCompressionFailureRemovesBlob(t *testing.T) {
	b := setupTestBackend(t)
	b.compressAlgo = closeFailCompression{}
	usr := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(usr.username, textproto.Header{}))
	assert.Assert(t, d.BodyRaw(strings.NewReader(testMsg)) != nil, "expected compression flush error")
	assert.NilError(t, d.Abort())

	assert.Assert(t, emptyExtStore(b.Store))
}

func TestDeliveryAbortRetriesFailedBodyCleanup(t *testing.T) {
	b := setupTestBackend(t)
	b.extStore = &retryDeleteStore{syncFailStore: syncFailStore{b.extStore}}
	usr := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(usr.username, textproto.Header{}))
	assert.Assert(t, d.BodyRaw(strings.NewReader(testMsg)) != nil, "expected sync error")
	assert.NilError(t, d.Abort())

	assert.Assert(t, emptyExtStore(b.Store))
}

func TestDeliveryInterleavedBodies(t *testing.T) {
	b := setupTestBackend(t)
	usr := b.user()

	d1 := b.NewDelivery()
	defer d1.Abort()

	d2 := b.NewDelivery()
	defer d2.Abort()

	for _, d := range []*Delivery{&d1, &d2} {
		assert.NilError(t, d.AddRcpt(usr.Username(), textproto.Header{}))
		assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))
	}

	assert.NilError(t, d2.Commit())
	assert.NilError(t, d1.Commit())

	status, mbox, err := usr.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		assert.NilError(t, err)
	}
	defer mbox.Close()

	assert.Equal(t, status.Messages, uint32(2))
}

func TestDeliveryAbortRemovesStagedBlobs(t *testing.T) {
	b, _ := setupTestBackendWithFSStore(t)
	u1 := b.user()
	u2 := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u1.username, textproto.Header{}))
	assert.NilError(t, d.AddRcpt(u2.username, textproto.Header{}))
	if err := d.BodyRaw(strings.NewReader(testMsg)); err != nil {
		assert.NilError(t, err)
	}
	if err := d.Abort(); err != nil {
		assert.NilError(t, err)
	}
	assert.Assert(t, emptyExtStore(b.Store))
}

func TestDeliveryCommitRejectsDeletedMailbox(t *testing.T) {
	b, _ := setupTestBackendWithFSStore(t)
	u := b.user()
	assert.NilError(t, u.CreateMailbox("target"))

	d := b.NewDelivery()
	defer d.Abort()
	assert.NilError(t, d.AddRcpt(u.username, textproto.Header{}))
	assert.NilError(t, d.Mailbox("target"))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))
	assert.NilError(t, u.DeleteMailbox("target"))
	assert.NilError(t, u.CreateMailbox("target"))
	assert.Assert(t, d.Commit() != nil, "commit succeeded after mailbox was deleted and recreated")

	assert.Assert(t, emptyExtStore(b.Store))

	status, mbox, err := u.GetMailbox("target", true, &noopConn{})
	assert.NilError(t, err)
	defer mbox.Close()

	assert.Equal(t, status.Messages, uint32(0))
}

func TestDeliveryCommitFailureRollsBackEveryRecipient(t *testing.T) {
	b, _ := setupTestBackendWithFSStore(t)
	u1, u2 := b.user(), b.user()

	_, selected, err := u1.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		assert.NilError(t, err)
	}
	defer selected.Close()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u1.username, textproto.Header{}))
	assert.NilError(t, d.AddRcpt(u2.username, textproto.Header{}))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

	assert.NilError(t, b.DeleteUser(u2.username))

	assert.Assert(t,
		errors.Is(d.Commit(), ErrDeliveryInterrupted),
		"commit after deletion must return ErrDeliveryInterrupted",
	)

	assert.Assert(t, emptyExtStore(b.Store))

	status, err := u1.Status("INBOX", []imap.StatusItem{imap.StatusMessages})
	if err != nil {
		assert.NilError(t, err)
	}
	assert.Equal(t, status.Messages, uint32(0))

	selected.(*Mailbox).handle.Sync(true)
	seq := new(imap.SeqSet)
	seq.AddNum(1)

	resolved, err := selected.(*Mailbox).handle.ResolveSeq(false, seq)
	assert.Assert(t, err != nil || resolved.Empty(), "selected mailbox observed rolled-back messages: %v", resolved)
}

func TestDeliveryConcurrentNotificationsStayInUIDOrder(t *testing.T) {
	b := setupTestBackendForConcurrentSQLite(t)
	u := b.user()

	_, selected, err := u.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		assert.NilError(t, err)
	}
	defer selected.Close()

	const count = 20
	deliveries := make([]Delivery, count)
	for i := range deliveries {
		deliveries[i] = b.NewDelivery()
		assert.NilError(t, deliveries[i].AddRcpt(u.Username(), textproto.Header{}))
		assert.NilError(t, deliveries[i].BodyRaw(strings.NewReader(testMsg)))
	}

	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := range deliveries {
		wg.Go(func() {
			err := deliveries[i].Commit()
			t.Log(i, "delivery:", err)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NilError(t, err)
	}

	handle := selected.(*Mailbox).handle
	handle.Sync(true)
	for uid := uint32(1); uid <= count; uid++ {
		seq, ok := handle.UidAsSeq(uid)
		assert.Assert(t, ok && seq == uid, "UID %d has sequence %d, present=%t", uid, seq, ok)
	}
}

func TestDeliverySelectDuringCommitDoesNotDuplicateUID(t *testing.T) {
	b := setupTestBackendForConcurrentSQLite(t)
	u := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

	updates := make(chan mess.Update)

	// SetExternalSink can block
	b.mngr.SetExternalSink(updates)
	committed := make(chan error, 1)
	go func() { committed <- d.Commit() }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		assert.NilError(t, b.DB.QueryRow(`SELECT COUNT(*) FROM msgs`).Scan(&count))
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
	assert.NilError(t, <-committed)
	if r.box == nil && r.err == nil {
		r = <-selected
	}
	assert.NilError(t, r.err)
	defer r.box.Close()

	handle := r.box.(*Mailbox).handle
	handle.Sync(true)

	seq := new(imap.SeqSet)
	seq.AddNum(2)
	resolved, err := handle.ResolveSeq(false, seq)
	assert.Assert(t, err != nil || resolved.Empty(), "one committed message appeared twice: %v", resolved)
}

func TestAppendCommitFailureDoesNotNotify(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	_, selected, err := u.GetMailbox("INBOX", false, &noopConn{})
	if err != nil {
		assert.NilError(t, err)
	}
	defer selected.Close()
	mbox := selected.(*Mailbox)

	// Force tx.Commit to fail via deferred FK violation.
	for _, query := range []string{
		`CREATE TABLE commit_parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE commit_child (id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER commit_failure AFTER INSERT ON msgs BEGIN INSERT INTO commit_child VALUES (1); END`,
	} {
		_, err := b.DB.Exec(query)
		assert.NilError(t, err)
	}

	assert.Assert(t, mbox.CreateMessage(nil, time.Now(), strings.NewReader(testMsg)) != nil, "expected commit failure")

	mbox.handle.Sync(true)
	seq := new(imap.SeqSet)
	seq.AddNum(1)

	resolved, err := mbox.handle.ResolveSeq(false, seq)
	assert.Assert(t, err != nil || resolved.Empty(), "selected mailbox observed rolled-back message: %v", resolved)
}

func TestDeliveryOutOfOrderNotificationsReconcileUIDs(t *testing.T) {
	b := setupTestBackend(t)
	assert.NilError(t, b.CreateUser("user"))
	u, err := b.GetUser("user")
	assert.NilError(t, err)
	_, selected, err := u.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	defer selected.Close()
	mbox := selected.(*Mailbox)

	// Commit two real staged messages, then deliberately deliver their hints
	// in reverse order with a mailbox sync between them.

	for range 2 {
		d := b.NewDeliveryWithOptions(DeliveryOptions{SkipNotify: true})
		assert.NilError(t, d.AddRcpt("user", textproto.Header{}))
		assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))
		assert.NilError(t, d.Commit())
	}

	b.mngr.NewMessage(mbox.id, 2)
	mbox.handle.Sync(true)
	for uid := uint32(1); uid <= 2; uid++ {
		seq, ok := mbox.handle.UidAsSeq(uid)
		assert.Assert(t, ok && seq == uid, "after later hint, UID %d has sequence %d, present=%t", uid, seq, ok)
	}

	b.mngr.NewMessage(mbox.id, 1)
	mbox.handle.Sync(true)
	seq := new(imap.SeqSet)
	seq.AddNum(3)
	resolved, err := mbox.handle.ResolveSeq(false, seq)
	assert.Assert(t, err != nil || resolved.Empty(), "late hint duplicated a message: %v", resolved)
}

func TestDeliveryRecentClaimedByOneSelectedSession(t *testing.T) {
	b := setupTestBackend(t)
	assert.NilError(t, b.CreateUser("user"))
	u, err := b.GetUser("user")
	assert.NilError(t, err)

	_, first, err := u.GetMailbox("INBOX", false, &noopConn{})
	assert.NilError(t, err)
	defer first.Close()

	_, second, err := u.GetMailbox("INBOX", false, &noopConn{})
	assert.NilError(t, err)
	defer second.Close()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt("user", textproto.Header{}))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))
	assert.NilError(t, d.Commit())

	first.(*Mailbox).handle.Sync(true)
	second.(*Mailbox).handle.Sync(true)

	aRecent := first.(*Mailbox).handle.IsRecent(1)
	bRecent := second.(*Mailbox).handle.IsRecent(1)
	assert.Assert(t, aRecent != bRecent, "expected exactly one recent owner; first=%t second=%t", aRecent, bRecent)

	status, later, err := u.GetMailbox("INBOX", false, &noopConn{})
	assert.NilError(t, err)
	defer later.Close()

	assert.Equal(t, status.Recent, uint32(0))
}

func TestSelectedSnapshotIncludesLargeMailbox(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	_, err := b.db.Exec(`WITH RECURSIVE nums(n) AS (
		SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 100001
	) INSERT INTO msgs(mboxId, msgId, date, bodyLen, bodyStructure, cachedHeader, seen, recent)
	SELECT (SELECT inboxId FROM users WHERE username = ?), n, 0, 0, '{}', '{}', 0, 0 FROM nums`,
		u.Username())
	if err != nil {
		assert.NilError(t, err)
	}

	status, selected, err := u.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	defer selected.Close()

	assert.Equal(t, status.Messages, uint32(100001))
}

func TestDeliveryNewMailboxCreatedBeforeCommitIsTemporary(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
	// Will select mbox ID if it already exists, but will defer creation to Commit if it does not exist.
	assert.NilError(t, d.Mailbox("new-folder"))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

	assert.NilError(t, u.CreateMailbox("new-folder"))

	assert.Assert(t, errors.Is(d.Commit(), ErrDeliveryInterrupted), "commit after concurrent mailbox creation must return ErrDeliveryInterrupted")
}

func TestDeliveryRejectsStaleSpecialMailbox(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	assert.NilError(t, u.CreateMailboxSpecial("junk", imap.JunkAttr))

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))

	// Will select mbox ID of "junk".
	assert.NilError(t, d.SpecialMailbox(imap.JunkAttr, "junk"))

	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

	// Reset special use.
	_, err := b.DB.Exec(`UPDATE mboxes SET specialuse = NULL WHERE name = ?`, "junk")
	assert.NilError(t, err)

	// The folder is no longer Junk, cannot send Junk message there.
	assert.Assert(t, errors.Is(d.Commit(), ErrDeliveryInterrupted), "commit after special-use change must return ErrDeliveryInterrupted")
}

func TestDeliveryReuseAfterAbortAndCommit(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	d := b.NewDelivery()
	for i := range 3 {
		assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
		assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

		if i == 0 {
			assert.NilError(t, d.Abort())
		} else {
			assert.NilError(t, d.Commit())
		}
	}

	status, mbox, err := u.GetMailbox("INBOX", true, &noopConn{})
	if err != nil {
		assert.NilError(t, err)
	}
	defer mbox.Close()

	assert.Equal(t, status.Messages, uint32(2))
}

func TestDeliveryDuplicateRecipientNewMailbox(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	d := b.NewDelivery()
	for range 2 {
		assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
	}
	assert.NilError(t, d.Mailbox("new-folder"))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))
	assert.NilError(t, d.Commit())

	status, mbox, err := u.GetMailbox("new-folder", true, &noopConn{})
	if err != nil {
		assert.NilError(t, err)
	}
	defer mbox.Close()

	assert.Equal(t, status.Messages, uint32(2))
}

func TestDeliveryCommitBusyCleansUp(t *testing.T) {
	b, _ := setupTestBackendWithFSStore(t)
	u := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

	tx, err := b.db.Begin(false)
	assert.NilError(t, err)

	assert.Assert(t, d.Commit() != nil, "expected commit to fail while database is locked")

	assert.NilError(t, tx.Rollback())

	assert.Assert(t, emptyExtStore(b.Store))
	assert.NilError(t, d.Abort())
}

func TestDeliveryRejectsUnsupportedSpecialMailbox(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
	assert.Assert(t, errors.Is(d.SpecialMailbox(`\All`, "all"), ErrUnsupportedSpecialAttr), "unsupported special mailbox must return ErrUnsupportedSpecialAttr")
}

func TestDeliverySQLCommitFailureCleansUp(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	// Force Commit failure via deferred FK violation.
	for _, query := range []string{
		`CREATE TABLE commit_parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE commit_child (id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER commit_failure AFTER INSERT ON msgs BEGIN INSERT INTO commit_child VALUES (1); END`,
	} {
		_, err := b.DB.Exec(query)
		assert.NilError(t, err)
	}
	defer func() {
		for _, query := range []string{
			`DROP TRIGGER commit_failure`,
			`DROP TABLE commit_child`,
			`DROP TABLE commit_parent`,
		} {
			_, err := b.DB.Exec(query)
			assert.NilError(t, err)
		}
	}()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

	assert.Assert(t, d.Commit() != nil, "expected deferred constraint to fail at commit")
	assert.Assert(t, emptyExtStore(b.Store))

	var count int
	assert.NilError(t, b.DB.QueryRow(`SELECT COUNT(*) FROM msgs`).Scan(&count))
	assert.Equal(t, count, 0)
}

func TestDeliveryAbortAfterFailedCommitPreservesReferencedBlob(t *testing.T) {
	b := setupTestBackend(t)
	u := b.user()

	d := b.NewDelivery()
	assert.NilError(t, d.AddRcpt(u.Username(), textproto.Header{}))
	assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))

	// Simulate a driver reporting an error after the server committed.
	tx, err := b.db.Begin(false)
	assert.NilError(t, err)
	assert.NilError(t, d.commitMailbox(tx, &d.staged[0]))
	assert.NilError(t, tx.Commit())

	d.commitFailed = true
	if err := d.Abort(); err != nil {
		assert.NilError(t, err)
	}

	assert.Equal(t, b.Store.(*testStore).Len(), 1)
}
