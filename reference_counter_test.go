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
		dirList, err := ioutil.ReadDir(b.extStore.(*FSStore).Root)
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
	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox(t.Name()))
	_, mbox, err := usr.GetMailbox(t.Name(), true, &noopConn{})
	assert.NilError(t, err)
	defer mbox.Close()

	// Message is created, there should be a key.
	assert.NilError(t, usr.CreateMessage(mbox.Name(), []string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.NilError(t, mbox.Poll(true))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// Message is removed, there should be no key anymore.
	assert.NilError(t, mbox.Expunge())
	assert.Assert(t, checkKeysCount(b, 0), "Key is not removed after message removal")
}

func TestKeyIsRemovedWithMbox(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox(t.Name()))
	_, mbox, err := usr.GetMailbox(t.Name(), true, &noopConn{})
	assert.NilError(t, err)

	// Message is created, there should be a key.
	assert.NilError(t, usr.CreateMessage(mbox.Name(), []string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.NilError(t, mbox.Poll(true))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// The mbox is removed along with all messages, there should be no key anymore.
	assert.NilError(t, usr.DeleteMailbox(t.Name()))
	assert.Assert(t, checkKeysCount(b, 0), "Key is not removed after mbox removal")
}

func TestKeyIsRemovedWithCopiedMsgs(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)

	assert.NilError(t, usr.CreateMailbox(t.Name()+"-1"))
	_, mbox1, err := usr.GetMailbox(t.Name()+"-1", true, &noopConn{})
	assert.NilError(t, err)
	defer mbox1.Close()

	assert.NilError(t, usr.CreateMailbox(t.Name()+"-2"))
	_, mbox2, err := usr.GetMailbox(t.Name()+"-2", true, &noopConn{})
	assert.NilError(t, err)
	defer mbox2.Close()

	// The message is created, there should be a key.
	assert.NilError(t, usr.CreateMessage(mbox1.Name(), []string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.NilError(t, mbox1.Poll(true))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// The message is copied, there should be no duplicate key.
	seq, _ := imap.ParseSeqSet("1")
	assert.NilError(t, mbox1.CopyMessages(false, seq, mbox2.Name()))
	assert.NilError(t, mbox2.Poll(true))
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
	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox(t.Name()))
	_, mbox, err := usr.GetMailbox(t.Name(), true, &noopConn{})
	assert.NilError(t, err)
	defer mbox.Close()

	// The message is created, there should be a key.
	assert.NilError(t, usr.CreateMessage(mbox.Name(), []string{imap.DeletedFlag}, time.Now(), strings.NewReader(testMsg)))
	assert.Assert(t, checkKeysCount(b, 1), "Wrong amount of external store keys created")

	// The user account is removed, all keys should be gone.
	assert.NilError(t, b.DeleteUser(usr.Username()))
	assert.Assert(t, checkKeysCount(b, 0), "Key is not removed after message removal")
}
