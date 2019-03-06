package testsuite

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/google/go-cmp/cmp"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func getNamedUser(t *testing.T, b Backend, name string) backend.User {
	err := b.CreateUser(name, "password1")
	assert.NilError(t, err)
	u, err := b.GetUser(name)
	assert.NilError(t, err)
	return u
}

func getUser(t *testing.T, b Backend) backend.User {
	name := fmt.Sprintf("%s-%v", t.Name(), time.Now().UnixNano())
	return getNamedUser(t, b, name)
}

func getNamedMbox(t *testing.T, u backend.User, name string) backend.Mailbox {
	assert.NilError(t, u.CreateMailbox(name))
	mbox, err := u.GetMailbox(name)
	assert.NilError(t, err)
	return mbox
}

func getMbox(t *testing.T, u backend.User) backend.Mailbox {
	name := fmt.Sprintf("%s-%v", t.Name(), time.Now().UnixNano())
	return getNamedMbox(t, u, name)
}

var baseDate = time.Time{}

func createMsgs(t *testing.T, mbox backend.Mailbox, count int) {
	for i := 0; i < count; i++ {
		assert.NilError(t, mbox.CreateMessage(
			[]string{
				"$Test" + strconv.Itoa(1+2*i),
				"$Test" + strconv.Itoa(2+2*i),
			},
			baseDate.Add(time.Duration((i+1)*24)*time.Hour),
			strings.NewReader(testMailString),
		))
	}
}

func createMsgsUids(t *testing.T, mbox backend.Mailbox, count int) (res []uint32) {
	for i := 0; i < count; i++ {
		stat, err := mbox.Status([]imap.StatusItem{imap.StatusUidNext})
		assert.NilError(t, err)
		res = append(res, stat.UidNext)

		assert.NilError(t, mbox.CreateMessage(
			[]string{
				"$Test" + strconv.Itoa(1+2*i),
				"$Test" + strconv.Itoa(2+2*i),
			},
			baseDate.Add(time.Duration((i+1)*24)*time.Hour),
			strings.NewReader(testMailString),
		))
	}
	return
}

func isNthMsg(msg *imap.Message, indx int, args ...cmp.Option) is.Comparison {
	indx = indx - 1

	msgDate := msg.InternalDate.Truncate(time.Second)
	nthDate := baseDate.Add(time.Duration(indx+1) * 24 * time.Hour).Truncate(time.Second)

	return is.DeepEqual(msgDate, nthDate, args...)
}

func isNthMsgFlags(msg *imap.Message, indx int, args ...cmp.Option) is.Comparison {
	indx = indx - 1

	flags := []string{
		"$Test" + strconv.Itoa(1+2*indx),
		"$Test" + strconv.Itoa(2+2*indx),
		imap.RecentFlag,
	}
	return is.DeepEqual(msg.Flags, []string{flags[0], flags[1], imap.RecentFlag})
}
