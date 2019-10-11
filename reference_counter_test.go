package imapsql

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func checkKeysCount(b *Backend, expected int) is.Comparison {
	return func() is.Result {
		dirList, err := ioutil.ReadDir(b.Opts.ExternalStore.(*Store).Root)
		if err != nil {
			return is.ResultFromError(err)
		}
		if len(dirList) != expected {
			names := make([]string, 0, len(dirList))
			for _, ent := range dirList {
				names = append(names, ent.Name())
			}
			return is.ResultFailure(fmt.Sprintf("expected %d keys to be stored, got %d: %v", expected, len(dirList), names))
		}
		return is.ResultSuccess
	}
}

func TestKeyIsRemovedWithMsg(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox(t.Name()))
	mbox, err := usr.GetMailbox(t.Name())
	assert.NilError(t, err)

	// Message is created, there should be a key.
	assert.NilError(t, mbox.CreateMessage([]string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// Message is removed, there should be no key anymore.
	assert.NilError(t, mbox.Expunge())
	assert.Assert(t, checkKeysCount(b, 0), "Key is not removed after message removal")
}

func TestKeyIsRemovedWithMbox(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox(t.Name()))
	mbox, err := usr.GetMailbox(t.Name())
	assert.NilError(t, err)

	// Message is created, there should be a key.
	assert.NilError(t, mbox.CreateMessage([]string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// The mbox is removed along with all messages, there should be no key anymore.
	assert.NilError(t, usr.DeleteMailbox(t.Name()))
	assert.Assert(t, checkKeysCount(b, 0), "Key is not removed after mbox removal")
}

func TestKeyIsRemovedWithCopiedMsgs(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)

	assert.NilError(t, usr.CreateMailbox(t.Name()+"-1"))
	mbox1, err := usr.GetMailbox(t.Name() + "-1")
	assert.NilError(t, err)

	assert.NilError(t, usr.CreateMailbox(t.Name()+"-2"))
	mbox2, err := usr.GetMailbox(t.Name() + "-2")
	assert.NilError(t, err)

	// The message is created, there should be a key.
	assert.NilError(t, mbox1.CreateMessage([]string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// The message is copied, there should be no duplicate key.
	seq, _ := imap.ParseSeqSet("1")
	assert.NilError(t, mbox1.CopyMessages(false, seq, mbox2.Name()))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys")

	// The message copy is removed, key should be still here.
	assert.NilError(t, mbox2.Expunge())
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys")

	// Both messages are deleted, there should be no key anymore.
	assert.NilError(t, mbox1.Expunge())
	assert.Assert(t, checkKeysCount(b, 0), "Key is not removed after message removal")
}

func TestKeyIsRemovedWithUser(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox(t.Name()))
	mbox, err := usr.GetMailbox(t.Name())
	assert.NilError(t, err)

	// The message is created, there should be a key.
	assert.NilError(t, mbox.CreateMessage([]string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// The user account is removed, all keys should be gone.
	assert.NilError(t, b.DeleteUser(usr.Username()))
	assert.Assert(t, checkKeysCount(b, 0), "Key is not removed after message removal")
}
