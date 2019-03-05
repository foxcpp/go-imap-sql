package testsuite

import (
	"testing"

	"github.com/emersion/go-imap/backend"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func User_Username(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.Equal(t, u.Username(), "username1", "Username mismatch")
}

func User_CreateMailbox(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	mboxes, err := u.ListMailboxes(false)
	assert.NilError(t, err)
	assert.Assert(t, is.Len(mboxes, 0), "Non-empty mailboxes list after user creation")

	assert.NilError(t, u.CreateMailbox("INBOX"))

	mboxes, err = u.ListMailboxes(false)
	assert.NilError(t, err)
	assert.Assert(t, is.Len(mboxes, 1), "Unexpected length of mailboxes list after mailbox creation")

	mbox, err := u.GetMailbox("INBOX")
	assert.NilError(t, err)

	assert.Equal(t, mbox.Name(), "INBOX", "Mailbox name mismatch")
}

func User_CreateMailbox_Parents(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("INBOX.FOOBAR.BAR"))

	mboxes, err := u.ListMailboxes(false)
	assert.NilError(t, err)
	assert.Assert(t, is.Len(mboxes, 3), "Unexpected length of mailboxes list after mailbox creation")

	mbox, err := u.GetMailbox("INBOX.FOOBAR.BAR")
	assert.NilError(t, err)
	assert.Equal(t, mbox.Name(), "INBOX.FOOBAR.BAR", "Mailbox name mismatch")

	mbox, err = u.GetMailbox("INBOX.FOOBAR")
	assert.NilError(t, err)
	assert.Equal(t, mbox.Name(), "INBOX.FOOBAR", "Mailbox name mismatch")

	mbox, err = u.GetMailbox("INBOX")
	assert.NilError(t, err)
	assert.Equal(t, mbox.Name(), "INBOX", "Mailbox name mismatch")
}

func User_DeleteMailbox(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST"))
	assert.NilError(t, u.DeleteMailbox("TEST"))
	assert.Error(t, u.DeleteMailbox("TEST"), backend.ErrNoSuchMailbox.Error(), "User.DeleteMailbox succeed")
}

func User_DeleteMailbox_Parents(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST.FOOBAR.FOO"))
	assert.NilError(t, u.DeleteMailbox("TEST"))
	_, err = u.GetMailbox("TEST.FOOBAR.FOO")
	assert.NilError(t, err)
	_, err = u.GetMailbox("TEST.FOOBAR")
	assert.NilError(t, err)
}

func User_RenameMailbox(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST"))
	assert.NilError(t, u.RenameMailbox("TEST", "TEST2"))
	_, err = u.GetMailbox("TEST")
	assert.Error(t, err, backend.ErrNoSuchMailbox.Error(), "Mailbox with old name still exists")
	mbox, err := u.GetMailbox("TEST2")
	assert.NilError(t, err, "Mailbox with new name doesn't exists")
	assert.Equal(t, mbox.Name(), "TEST2", "Mailbox name dismatch in returned object")
}

func User_RenameMailbox_Childrens(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)

	assert.NilError(t, u.CreateMailbox("TEST.FOOBAR.BAR"))
	assert.NilError(t, u.RenameMailbox("TEST", "TEST2"))
	mbox, err := u.GetMailbox("TEST2.FOOBAR.BAR")
	assert.NilError(t, err, "Mailbox children with new name doesn't exists")
	assert.Equal(t, mbox.Name(), "TEST2.FOOBAR.BAR", "Mailbox name dismatch in returned object")
	mbox, err = u.GetMailbox("TEST2.FOOBAR")
	assert.NilError(t, err, "Mailbox children with new name doesn't exists")
	assert.Equal(t, mbox.Name(), "TEST2.FOOBAR", "Mailbox name dismatch in returned object")
}

func User_RenameMailbox_INBOX(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)

	assert.NilError(t, u.CreateMailbox("INBOX"))
	assert.NilError(t, u.RenameMailbox("INBOX", "TEST2"))
	_, err = u.GetMailbox("INBOX")
	assert.NilError(t, err, "INBOX doesn't exists anymore")
}

func User_ListMailboxes(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)

	assert.NilError(t, u.CreateMailbox("INBOX"))

	mboxes, err := u.ListMailboxes(false)
	assert.NilError(t, err)
	assert.Assert(t, is.Len(mboxes, 1), "Mailboxes count mismatch")
	assert.Equal(t, mboxes[0].Name(), "INBOX", "Mailbox name mismatch")
}
