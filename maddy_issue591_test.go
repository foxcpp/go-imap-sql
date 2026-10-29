package imapsql

import (
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"gotest.tools/assert"
)

func TestSearchUnseenDoesNotUseFlagsFromOtherMailbox(t *testing.T) {
	b := initTestBackend()
	defer cleanBackend(b)

	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox("seen"))
	assert.NilError(t, usr.CreateMailbox("unseen"))

	// UIDs are mailbox-local, so both messages receive UID 1. The flag on
	// the first mailbox must not affect SEARCH UNSEEN in the second mailbox.
	assert.NilError(t, usr.CreateMessage("seen", []string{imap.SeenFlag}, time.Now(), strings.NewReader(testMsg), nil))
	assert.NilError(t, usr.CreateMessage("unseen", nil, time.Now(), strings.NewReader(testMsg), nil))

	_, unseenMailbox, err := usr.GetMailbox("unseen", true, &noopConn{})
	assert.NilError(t, err)

	result, err := unseenMailbox.SearchMessages(true, &imap.SearchCriteria{
		WithoutFlags: []string{imap.SeenFlag},
	})
	assert.NilError(t, err)
	assert.DeepEqual(t, result, []uint32{1})
}
