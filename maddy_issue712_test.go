package imapsql

import (
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func TestFetchBodyMarksMessageSeenPersistently(t *testing.T) {
	b := setupTestBackend(t)

	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMessage("INBOX", nil, time.Now(), strings.NewReader(testMsg), nil))

	_, mailbox, err := usr.GetMailbox("INBOX", false, &noopConn{})
	assert.NilError(t, err)
	mbox := mailbox.(*Mailbox)
	defer mbox.Close()

	seq, err := imap.ParseSeqSet("1")
	assert.NilError(t, err)

	ch := make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{"BODY[1]"}, ch))
	assert.Assert(t, is.Contains((<-ch).Flags, imap.SeenFlag))

	ch = make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch))
	assert.Assert(t, is.Contains((<-ch).Flags, imap.SeenFlag), "\\Seen must remain set after the BODY FETCH transaction")
}
