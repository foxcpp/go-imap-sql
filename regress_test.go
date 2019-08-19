package imapsql

import (
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"gotest.tools/assert"
)

const (
	testMsgHeader = "From: <foxcpp@foxcpp.dev>\r\n" +
		"Subject: Hello!\r\n" +
		"Content-Type: text/plain; charset=ascii\r\n" +
		"Non-Cached-Header: 1\r\n" +
		"\r\n"
	testMsgBody = "Hello!\r\n"
	testMsg     = testMsgHeader +
		testMsgBody
)

func TestIssue7(t *testing.T) {
	b := initTestBackend()
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMailbox(t.Name()))
	mbox, err := usr.GetMailbox(t.Name())
	assert.NilError(t, err)
	for i := 0; i < 5; i++ {
		assert.NilError(t, mbox.CreateMessage([]string{"flag1", "flag2"}, time.Now(), strings.NewReader(testMsg)))
	}

	t.Run("seq", func(t *testing.T) {
		crit := imap.SearchCriteria{}
		seqs, err := mbox.SearchMessages(false, &crit)
		assert.NilError(t, err)

		t.Log("Seq. nums.:", seqs)

		seenSeq := make(map[uint32]bool)
		for _, seq := range seqs {
			assert.Check(t, !seenSeq[seq], "Duplicate sequence number in SEARCH ALL response")
			seenSeq[seq] = true
		}
	})
	t.Run("uid", func(t *testing.T) {
		crit := imap.SearchCriteria{}
		uids, err := mbox.SearchMessages(true, &crit)
		assert.NilError(t, err)

		t.Log("UIDs:", uids)

		seenUids := make(map[uint32]bool)
		for _, uid := range uids {
			assert.Check(t, !seenUids[uid], "Duplicate UID in SEARCH ALL response")
			seenUids[uid] = true
		}
	})
}
