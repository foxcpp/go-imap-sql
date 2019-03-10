package testsuite

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
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
	t.Helper()
	err := b.CreateUser(name, "password1")
	assert.NilError(t, err)
	u, err := b.GetUser(name)
	assert.NilError(t, err)
	return u
}

func getUser(t *testing.T, b Backend) backend.User {
	t.Helper()
	name := fmt.Sprintf("%s-%v", t.Name(), time.Now().UnixNano())
	return getNamedUser(t, b, name)
}

func getNamedMbox(t *testing.T, u backend.User, name string) backend.Mailbox {
	t.Helper()
	assert.NilError(t, u.CreateMailbox(name))
	mbox, err := u.GetMailbox(name)
	assert.NilError(t, err)
	return mbox
}

func getMbox(t *testing.T, u backend.User) backend.Mailbox {
	t.Helper()
	name := fmt.Sprintf("%s-%v", t.Name(), time.Now().UnixNano())
	return getNamedMbox(t, u, name)
}

var baseDate = time.Time{}

func createMsgs(t *testing.T, mbox backend.Mailbox, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		assert.NilError(t, mbox.CreateMessage(
			[]string{
				"$Test" + strconv.Itoa(i+1) + "-1",
				"$Test" + strconv.Itoa(i+1) + "-2",
			},
			baseDate.Add(time.Duration((i+1)*24)*time.Hour),
			strings.NewReader(testMailString),
		))
	}
}

func createMsgsUids(t *testing.T, mbox backend.Mailbox, count int) (res []uint32) {
	t.Helper()
	for i := 0; i < count; i++ {
		stat, err := mbox.Status([]imap.StatusItem{imap.StatusUidNext})
		assert.NilError(t, err)
		res = append(res, stat.UidNext)

		assert.NilError(t, mbox.CreateMessage(
			[]string{
				"$Test" + strconv.Itoa(i+1) + "-1",
				"$Test" + strconv.Itoa(i+1) + "-2",
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
	sort.Strings(msg.Flags)
	flags := []string{
		"$Test" + strconv.Itoa(indx) + "-1",
		"$Test" + strconv.Itoa(indx) + "-2",
		imap.RecentFlag,
	}
	return is.DeepEqual(msg.Flags, []string{flags[0], flags[1], imap.RecentFlag})
}

func init() {
	if os.Getenv("SHUFFLE_CASES") == "1" {
		rand.Seed(time.Now().Unix())
	}
}
