package imapsql

import (
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestUserCaseInsensitivity(t *testing.T) {
	b := setupTestBackend(t)

	assert.NilError(t, b.CreateUser("foXcpp"))
	_, err := b.Login(nil, "foXCpp", "")
	assert.NilError(t, err, "b.Login")
	u1, err := b.GetUser("Foxcpp")
	assert.NilError(t, err, "b.GetUser")
	u2, err := b.GetOrCreateUser("FOXcpp")
	assert.NilError(t, err, "b.GetOrCreateUser")

	assert.NilError(t, u1.CreateMailbox("BOX"))
	_, mbox, err := u2.GetMailbox("BOX", true, &noopConn{})
	assert.NilError(t, err, "u2.GetMailbox")
	defer mbox.Close()
}

func TestInboxCreation(t *testing.T) {
	b := setupTestBackend(t)

	assert.NilError(t, b.CreateUser("foxcpp"))

	u, err := b.GetUser("foxcpp")
	assert.NilError(t, err, "b.GetUser")

	_, mbox, err := u.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err, "u.GetMailbox")
	defer mbox.Close()
}

func TestRenameMailboxIntoOwnChild(t *testing.T) {
	b := setupTestBackend(t)

	assert.NilError(t, b.CreateUser(t.Name()))
	u, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, u.CreateMailbox("w58d"))

	err = u.RenameMailbox("w58d", "w58d.child")
	assert.Assert(t, err != nil, "renaming a mailbox into its own child must fail")

	_, mbox, err := u.GetMailbox("w58d", false, &noopConn{})
	assert.NilError(t, err)
	assert.NilError(t, mbox.Close())
	_, _, err = u.GetMailbox("w58d.child", false, &noopConn{})
	assert.Assert(t, err != nil, "failed rename must not create the destination mailbox")
	_, _, err = u.GetMailbox("w58d.child.child", false, &noopConn{})
	assert.Assert(t, err != nil, "rename must not create a nested mailbox")
}

func TestRenameInboxUpdatesCachedMailboxID(t *testing.T) {
	b := setupTestBackend(t)
	assert.NilError(t, b.CreateUser(t.Name()))
	u, err := b.GetUser(t.Name())
	assert.NilError(t, err)

	for i := 0; i < 2; i++ {
		assert.NilError(t, u.CreateMessage("INBOX", nil, time.Now(), strings.NewReader(testMsg), nil))
	}
	assert.NilError(t, u.RenameMailbox("INBOX", "OldInbox"))

	status, inbox, err := u.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	assert.Equal(t, status.Messages, uint32(0), "INBOX on the renaming user must refer to the new empty mailbox")
	assert.NilError(t, inbox.Close())

	status, archived, err := u.GetMailbox("OldInbox", true, &noopConn{})
	assert.NilError(t, err)
	assert.Equal(t, status.Messages, uint32(2), "renamed mailbox must retain its messages")
	assert.NilError(t, archived.Close())

	freshUser, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	status, freshInbox, err := freshUser.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	assert.Equal(t, status.Messages, uint32(0), "a fresh user must see the same empty INBOX")
	assert.NilError(t, freshInbox.Close())
}

func TestRenameNonInboxDoesNotChangeInbox(t *testing.T) {
	b := setupTestBackend(t)
	assert.NilError(t, b.CreateUser(t.Name()))
	u, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, u.CreateMailbox("Archive"))
	assert.NilError(t, u.RenameMailbox("Archive", "OldArchive"))

	status, inbox, err := u.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	assert.Equal(t, status.Messages, uint32(0), "renaming another mailbox must leave INBOX unchanged")
	assert.NilError(t, inbox.Close())

	_, oldArchive, err := u.GetMailbox("OldArchive", true, &noopConn{})
	assert.NilError(t, err)
	assert.NilError(t, oldArchive.Close())
}
