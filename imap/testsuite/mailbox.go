package testsuite

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	move "github.com/emersion/go-imap-move"
	"github.com/emersion/go-imap/backend"
	"github.com/foxcpp/go-imap-sql/imap/children"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func Mailbox_Info(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST"))
	mbox, err := u.GetMailbox("TEST")
	assert.NilError(t, err)

	assert.NilError(t, u.CreateMailbox("TESTC.TEST.FOOBAR"))
	mboxC, err := u.GetMailbox("TESTC")
	assert.NilError(t, err)

	info, err := mbox.Info()
	assert.NilError(t, err)
	assert.Equal(t, info.Name, mbox.Name(), "Mailbox name mismatch")

	t.Run("HasChildren attr", func(t *testing.T) {
		b, ok := b.(children.Backend)
		if !ok || !b.EnableChildrenExt() {
			t.Skip("CHILDREN extension is not implemeted")
			t.SkipNow()
		}

		info, err := mbox.Info()
		assert.NilError(t, err)
		checkMailboxChildrens(t, info, u, mbox)

		infoC, err := mboxC.Info()
		assert.NilError(t, err)
		checkMailboxChildrens(t, infoC, u, mboxC)
	})

}

func checkMailboxChildrens(t *testing.T, info *imap.MailboxInfo, u backend.User, mbox backend.Mailbox) {
	hasChildrenAttr := false
	hasNoChildrenAttr := false
	for _, attr := range info.Attributes {
		if attr == `\HasChildren` {
			hasChildrenAttr = true
		}
		if attr == `\HasNoChildren` {
			hasNoChildrenAttr = true
		}
	}
	mboxes, err := u.ListMailboxes(false)
	assert.NilError(t, err)
	hasChildren := false
	for _, mbx := range mboxes {
		if strings.HasPrefix(mbx.Name(), info.Name+info.Delimiter) {
			hasChildren = true
		}
	}
	if hasChildren {
		if !hasChildrenAttr {
			t.Error("\\HasChildren attribute is not present on directory with childrens")
		}
		if hasNoChildrenAttr {
			t.Error("\\HasNoChildren attribute is present on directory with childrens")
		}
	}
	if !hasChildren {
		if hasChildrenAttr {
			t.Error("\\HasChildren attribute is present on directory without childrens")
			t.FailNow()
		}
		if !hasNoChildrenAttr {
			t.Error("\\HasNoChildren attribute is not present on directory without childrens")
		}
	}
}

const testMsg = `To: test@test
From: test <test@test>
Subject: test
Date: Tue, 8 May 2018 20:48:21 +0000
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: 7bit
Cc: foo <foo@foo>, bar <bar@bar>
X-CustomHeader: foo

Test! Test! Test! Test!
`

func Mailbox_Status(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	t.Run("UidNext", func(t *testing.T) {
		b := newBack()
		defer closeBack(b)
		u := getUser(t, b)
		defer assert.NilError(t, u.Logout())
		mbox := getMbox(t, u)

		status, err := mbox.Status([]imap.StatusItem{imap.StatusUidNext})
		assert.NilError(t, err)

		date := time.Now()
		err = mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg))
		assert.NilError(t, err)

		seq := imap.SeqSet{}
		seq.AddNum(1)
		ch := make(chan *imap.Message, 1)
		assert.NilError(t, mbox.ListMessages(false, &seq, []imap.FetchItem{imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 1), "Missing message")
		msg := <-ch

		assert.Equal(t, msg.Uid, status.UidNext, "UIDNEXT is incorrect")
	})

	t.Run("Messages + Recent", func(t *testing.T) {
		b := newBack()
		defer closeBack(b)
		u := getUser(t, b)
		defer assert.NilError(t, u.Logout())
		mbox := getMbox(t, u)

		date := time.Now()
		err := mbox.CreateMessage([]string{"$Test3", "$Test4"}, date, strings.NewReader(testMsg))
		assert.NilError(t, err)

		date = time.Now()
		err = mbox.CreateMessage([]string{"$Test3", "$Test4"}, date, strings.NewReader(testMsg))
		assert.NilError(t, err)

		status, err := mbox.Status([]imap.StatusItem{imap.StatusRecent, imap.StatusMessages})
		assert.NilError(t, err)
		assert.Equal(t, status.Recent, uint32(2), "Recent is invalid")
		assert.Equal(t, status.Messages, uint32(2), "Messages is invalid")
	})

	t.Run("UnseenSeqNum", func(t *testing.T) {
		b := newBack()
		defer closeBack(b)
		u := getUser(t, b)
		defer assert.NilError(t, u.Logout())
		mbox := getMbox(t, u)

		date := time.Now()
		err := mbox.CreateMessage([]string{"$Test3", "$Test4", imap.SeenFlag}, date, strings.NewReader(testMsg))
		assert.NilError(t, err)

		date = time.Now()
		err = mbox.CreateMessage([]string{"$Test3", "$Test4"}, date, strings.NewReader(testMsg))
		assert.NilError(t, err)

		status, err := mbox.Status(nil)
		assert.NilError(t, err)
		assert.Equal(t, status.UnseenSeqNum, uint32(2), "UnseenSeqNum is invalid")
	})
}

func Mailbox_SetSubscribed(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	t.Run("SetSubscribed true", func(t *testing.T) {
		b := newBack()
		defer closeBack(b)
		u := getUser(t, b)
		defer assert.NilError(t, u.Logout())
		mbox := getMbox(t, u)

		assert.NilError(t, mbox.SetSubscribed(true))
		mboxes, err := u.ListMailboxes(true)
		assert.NilError(t, err)

		assert.Assert(t, len(mboxes) == 1 && mboxes[0].Name() == mbox.Name(), "Mailbox is not present in list when subscribed")
	})
	t.Run("SetSubscribed false", func(t *testing.T) {
		b := newBack()
		defer closeBack(b)
		u := getUser(t, b)
		defer assert.NilError(t, u.Logout())
		mbox := getMbox(t, u)

		assert.NilError(t, mbox.SetSubscribed(false))
		mboxes, err := u.ListMailboxes(true)
		assert.NilError(t, err)

		assert.Assert(t, len(mboxes) == 0, "Mailbox is present in list when unsubscribed")
	})
}

func Mailbox_CreateMessage(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	mbox := getMbox(t, u)

	status, err := mbox.Status([]imap.StatusItem{imap.StatusUidNext})
	assert.NilError(t, err)

	date := time.Now()
	err = mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg))
	assert.NilError(t, err)

	seq := imap.SeqSet{}
	seq.AddNum(status.UidNext)
	ch := make(chan *imap.Message, 1)
	err = mbox.ListMessages(true, &seq, []imap.FetchItem{imap.FetchInternalDate, imap.FetchFlags, imap.FetchBody, imap.FetchRFC822Size}, ch)
	assert.NilError(t, err)
	msg := <-ch

	sort.Strings(msg.Flags)

	assert.Assert(t, msg.InternalDate.Truncate(time.Second).Equal(date.Truncate(time.Second)), "InternalDate is not same")
	assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
	assert.Equal(t, uint32(len(testMsg)), msg.Size, "RFC822 size mismatch")
	for _, v := range msg.Body {
		b, err := ioutil.ReadAll(v)
		assert.NilError(t, err, "ReadAll body failed")
		assert.DeepEqual(t, testMsg, b)
	}
}

func Mailbox_ListMessages(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())
	mbox := getMbox(t, u)

	createMsgs(t, mbox, 3)

	testMsgs := func(uid bool, seqset string, expectedIndxes []int) {
		namePrefix := "Seq "
		if uid {
			namePrefix = "Uid "
		}

		t.Run(namePrefix+seqset, func(t *testing.T) {
			seq, _ := imap.ParseSeqSet(seqset)

			ch := make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(uid, seq, []imap.FetchItem{imap.FetchInternalDate, imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, len(expectedIndxes)), "Wrong number of messages returned")

			for i := 1; i <= len(expectedIndxes); i++ {
				msg, ok := <-ch
				assert.Check(t, ok, "Unexpected channel close")
				assert.Check(t, is.Equal(msg.SeqNum, uint32(expectedIndxes[i-1])), "SeqNum mismatch")
				assert.Check(t, isNthMsg(msg, expectedIndxes[i-1]), "Wrong message")
				assert.Check(t, isNthMsgFlags(msg, expectedIndxes[i-1]), "Wrong message flags")
			}
		})
	}

	cases := []struct {
		uid         bool
		seqset      string
		expectedIds []int
	}{
		{false, "1:5", []int{1, 2, 3}},
		{false, "1:*", []int{1, 2, 3}},
		{false, "*", []int{1, 2, 3}},
		{false, "1", []int{1}},
		{false, "2", []int{2}},
		{false, "3", []int{3}},
		{false, "1,3", []int{1, 3}},
		{false, "45:30", []int{}},
		{true, "1:3", []int{1, 2, 3}},
		{true, "1:5", []int{1, 2, 3}},
		{true, "1:*", []int{1, 2, 3}},
		{true, "*", []int{1, 2, 3}},
		{true, "1", []int{1}},
		{true, "2", []int{2}},
		{true, "3", []int{3}},
		{true, "1,3", []int{1, 3}},
		{true, "45:30", []int{}},
	}

	if os.Getenv("SHUFFLE_CASES") == "1" {
		rand.Shuffle(len(cases), func(i, j int) {
			cases[i], cases[j] = cases[j], cases[i]
		})
	}

	for _, case_ := range cases {
		testMsgs(case_.uid, case_.seqset, case_.expectedIds)
	}
}

func Mailbox_SetMessageFlags(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	testFlags := func(
		initialFlags [][]string, seqset string,
		uid bool, op imap.FlagsOp, opArgs []string,
		finalFlags [][]string) bool {

		return t.Run(fmt.Sprintf("uid=%v seqset=%v op=%v opArgs=%v", uid, seqset, op, opArgs), func(t *testing.T) {
			mbox := getMbox(t, u)
			for _, flagset := range initialFlags {
				assert.NilError(t, mbox.CreateMessage(flagset, time.Now(), strings.NewReader(testMsg)))
			}

			seq, err := imap.ParseSeqSet(seqset)
			if err != nil {
				panic(err)
			}
			assert.NilError(t, mbox.UpdateMessagesFlags(uid, seq, op, opArgs))

			seq, _ = imap.ParseSeqSet("*")
			ch := make(chan *imap.Message, len(initialFlags)+5)
			assert.NilError(t, mbox.ListMessages(uid, seq, []imap.FetchItem{imap.FetchUid, imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, len(finalFlags)))

			for i, flagset := range finalFlags {
				msg := <-ch

				sort.Strings(msg.Flags)
				sort.Strings(flagset)

				if !assert.Check(t, is.DeepEqual(msg.Flags, flagset), "Flags mismatch on %d message", i+1) {
					t.Log("msg.SeqNum:", msg.SeqNum)
					t.Log("msg.Uid:", msg.Uid)
					t.Log("msg.Flags:", msg.Flags)
					t.Log("Reference flag set:", flagset)
				}
			}
		})
	}

	cases := []struct {
		initialFlags [][]string
		seqset       string
		uid          bool
		op           imap.FlagsOp
		opArgs       []string
		finalFlags   [][]string
	}{
		// imap.AddFlags (uid = true)
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"*", true, imap.AddFlags, []string{"$Test3"},
			[][]string{
				{"$Test1", "$Test2", "$Test3", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
				{"$Test1", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"2:*", true, imap.AddFlags, []string{"$Test3"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
				{"$Test1", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"1", true, imap.AddFlags, []string{"$Test3"},
			[][]string{
				{"$Test1", "$Test2", "$Test3", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"2", true, imap.AddFlags, []string{"$Test3"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"3", true, imap.AddFlags, []string{"$Test3"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"1,3", true, imap.AddFlags, []string{"$Test3"},
			[][]string{
				{"$Test1", "$Test2", "$Test3", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"2,3", true, imap.AddFlags, []string{"$Test3"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
				{"$Test1", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"2,3", true, imap.AddFlags, []string{"$Test3", "$Test4"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test3", "$Test4", imap.RecentFlag},
				{"$Test1", "$Test3", "$Test4", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"2:3", true, imap.AddFlags, []string{"$Test3", "$Test4"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test3", "$Test4", imap.RecentFlag},
				{"$Test1", "$Test3", "$Test4", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"1:3", true, imap.AddFlags, []string{"$Test3", "$Test4"},
			[][]string{
				{"$Test1", "$Test2", "$Test3", "$Test4", imap.RecentFlag},
				{"$Test3", "$Test4", imap.RecentFlag},
				{"$Test1", "$Test3", "$Test4", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"1:5", true, imap.AddFlags, []string{"$Test3", "$Test4"},
			[][]string{
				{"$Test1", "$Test2", "$Test3", "$Test4", imap.RecentFlag},
				{"$Test3", "$Test4", imap.RecentFlag},
				{"$Test1", "$Test3", "$Test4", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
			"45:50", true, imap.AddFlags, []string{"$Test3", "$Test4"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test1", imap.RecentFlag},
			},
		},

		// imap.RemoveFlags (uid = false)
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
			"*", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", imap.RecentFlag},
				{imap.RecentFlag},
				{imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
			"2:*", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
			"1", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
			"2", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
			"1,3", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
			},
			"1,3", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"1:3", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"2:3", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"1:5", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", imap.RecentFlag},
				{imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"45:50", true, imap.RemoveFlags, []string{"$Test2"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
		},

		// imap.SetFlags (uid = true)
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"*", true, imap.SetFlags, []string{"$Test2", "$Test3"},
			[][]string{
				{"$Test2", "$Test3", imap.RecentFlag},
				{"$Test2", "$Test3", imap.RecentFlag},
				{"$Test2", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"2:*", true, imap.SetFlags, []string{"$Test2", "$Test3"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", "$Test3", imap.RecentFlag},
				{"$Test2", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"1", true, imap.SetFlags, []string{"$Test2", "$Test3"},
			[][]string{
				{"$Test2", "$Test3", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"2:3", true, imap.SetFlags, []string{"$Test2", "$Test3"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", "$Test3", imap.RecentFlag},
				{"$Test2", "$Test3", imap.RecentFlag},
			},
		},
		{
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
			"2", true, imap.SetFlags, []string{"$Test2", "$Test3"},
			[][]string{
				{"$Test1", "$Test2", imap.RecentFlag},
				{"$Test2", "$Test3", imap.RecentFlag},
				{"$Test3", imap.RecentFlag},
			},
		},
	}

	for _, case_ := range cases {
		case_.uid = false
		cases = append(cases, case_)
	}

	if os.Getenv("SHUFFLE_CASES") == "1" {
		rand.Shuffle(len(cases), func(i, j int) {
			cases[i], cases[j] = cases[j], cases[i]
		})
	}

	for _, case_ := range cases {
		testFlags(case_.initialFlags, case_.seqset, case_.uid, case_.op, case_.opArgs, case_.finalFlags)
	}
}

func Mailbox_Expunge(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())
	mbox := getMbox(t, u)
	createMsgs(t, mbox, 3)

	assert.NilError(t, mbox.Expunge())

	status, err := mbox.Status([]imap.StatusItem{imap.StatusMessages})
	assert.NilError(t, err)
	assert.Equal(t, status.Messages, uint32(3), "Expunge deleted non-flagged messages")

	seq, _ := imap.ParseSeqSet("2:3")
	assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.AddFlags, []string{imap.DeletedFlag}))

	assert.NilError(t, mbox.Expunge())

	seq, _ = imap.ParseSeqSet("*:*")
	ch := make(chan *imap.Message, 10)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchInternalDate}, ch))
	assert.Assert(t, is.Len(ch, 1), "Expunge didn't removed messages or removed more of them")
	msg := <-ch
	assert.Assert(t, isNthMsg(msg, 1), "Wrong messages deleted")
}

func Mailbox_CopyMessages(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	testCopy := func(uid bool, seqset string, expectedTgtRes []int) bool {
		return t.Run(fmt.Sprintf("uid=%v seqset=%v", uid, seqset), func(t *testing.T) {
			srcMbox, tgtMbox := getMbox(t, u), getMbox(t, u)
			createMsgs(t, srcMbox, 3)

			seq, err := imap.ParseSeqSet(seqset)
			if err != nil {
				panic(err)
			}
			assert.NilError(t, srcMbox.CopyMessages(uid, seq, tgtMbox.Name()))

			seq, _ = imap.ParseSeqSet("*")
			ch := make(chan *imap.Message, len(expectedTgtRes)+10)
			err = tgtMbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchInternalDate, imap.FetchFlags}, ch)
			assert.NilError(t, err)
			assert.Assert(t, is.Len(ch, len(expectedTgtRes)), "Wrong amount of messages copied")

			for i, indx := range expectedTgtRes {
				msg := <-ch
				assert.Check(t, isNthMsg(msg, indx), "Message %d in target mbox is not same as %d in source mbox", i+1, indx)
				assert.Check(t, isNthMsgFlags(msg, indx), "Message %d in target mbox is not same as %d in source mbox (flags don't match)", i+1, indx)
			}
		})
	}

	t.Run("Non-Existent Dest", func(t *testing.T) {
		srcMbox := getMbox(t, u)
		createMsgs(t, srcMbox, 3)
		seq, _ := imap.ParseSeqSet("2:3")
		assert.Error(t, srcMbox.CopyMessages(false, seq, "NONEXISTENT"), backend.ErrNoSuchMailbox.Error())
	})

	cases := []struct {
		uid         bool
		seqset      string
		expectedRes []int
	}{
		{false, "*", []int{1, 2, 3}},
		{false, "2:*", []int{2, 3}},
		{false, "1", []int{1}},
		{false, "1,3", []int{1, 3}},
		{false, "2", []int{2}},
		{false, "1:5", []int{1, 2, 3}},
		{false, "1,1:3", []int{1, 2, 3}},
		{false, "45:30", []int{}},
	}

	for _, case_ := range cases {
		case_.uid = true
		cases = append(cases, case_)
	}

	if os.Getenv("SHUFFLE_CASES") == "1" {
		rand.Shuffle(len(cases), func(i, j int) {
			cases[i], cases[j] = cases[j], cases[i]
		})
	}

	for _, case_ := range cases {
		testCopy(case_.uid, case_.seqset, case_.expectedRes)
	}

	t.Run("Recent flag", func(t *testing.T) {
		srcMbox, tgtMbox := getMbox(t, u), getMbox(t, u)
		createMsgs(t, srcMbox, 1)

		seq, err := imap.ParseSeqSet("1")
		if err != nil {
			panic(err)
		}

		assert.NilError(t, srcMbox.UpdateMessagesFlags(false, seq, imap.SetFlags, []string{"$Test1"}))
		assert.NilError(t, srcMbox.CopyMessages(false, seq, tgtMbox.Name()))

		ch := make(chan *imap.Message, 10)
		err = tgtMbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch)
		assert.NilError(t, err)
		assert.Assert(t, is.Len(ch, 1), "Wrong amount of messages copied")

		msg := <-ch
		sort.Strings(msg.Flags)
		assert.Check(t, is.DeepEqual(msg.Flags, []string{"$Test1", imap.RecentFlag}), "Recent flag is not set on copied messages")
	})
}

func Mailbox_UidValidity_On_Rename(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	// Server implementation may choice to maintain the same UIDNEXT and UIDVALIDITY
	// or invalidate UIDNEXT and assign new value to UIDVALIDITY.

	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	mboxSrc, mboxTgt := getMbox(t, u), getMbox(t, u)
	createMsgs(t, mboxSrc, 5)
	createMsgs(t, mboxTgt, 3)

	oldStatus, err := mboxTgt.Status([]imap.StatusItem{imap.StatusUidValidity, imap.StatusUidNext})
	assert.NilError(t, err)

	assert.NilError(t, u.DeleteMailbox(mboxTgt.Name()))
	assert.NilError(t, u.RenameMailbox(mboxSrc.Name(), mboxTgt.Name()))

	mboxTgt, err = u.GetMailbox(mboxTgt.Name())
	assert.NilError(t, err)

	newStatus, err := mboxTgt.Status([]imap.StatusItem{imap.StatusUidValidity, imap.StatusUidNext})
	assert.NilError(t, err)

	if oldStatus.UidValidity == newStatus.UidValidity {
		assert.Check(t, oldStatus.UidNext > newStatus.UidNext, "Older UIDNEXT is bigger than never, but UIDVALIDITY is same")
	}
}

func Mailbox_MoveMessages(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	tMbox := getMbox(t, u)
	if _, ok := tMbox.(move.Mailbox); !ok {
		t.Skip("MOVE extension is not implemented (need move.Mailbox extension)")
		t.SkipNow()
	}

	testMove := func(uid bool, seqset string, expectedSrcRes, expectedTgtRes []int) bool {
		return t.Run(fmt.Sprintf("uid=%v seqset=%v", uid, seqset), func(t *testing.T) {
			srcMbox, tgtMbox := getMbox(t, u), getMbox(t, u)
			createMsgs(t, srcMbox, 3)

			moveMbox := srcMbox.(move.Mailbox)

			seq, err := imap.ParseSeqSet(seqset)
			if err != nil {
				panic(err)
			}
			assert.NilError(t, moveMbox.MoveMessages(uid, seq, tgtMbox.Name()))

			seq, _ = imap.ParseSeqSet("*")
			ch := make(chan *imap.Message, len(expectedTgtRes)+10)
			err = tgtMbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchInternalDate, imap.FetchFlags}, ch)
			assert.NilError(t, err)
			assert.Assert(t, is.Len(ch, len(expectedTgtRes)), "Wrong amount of messages created in tgt mbox")

			for i, indx := range expectedTgtRes {
				msg := <-ch
				assert.Check(t, isNthMsg(msg, indx), "Message %d in target mbox is not same as %d in source mbox", i+1, indx)
				if !assert.Check(t, isNthMsgFlags(msg, indx), "Message %d in target mbox is not same as %d in source mbox (flags don't match)", i+1, indx) {
					t.Log("msg.Flags:", msg.Flags)
					t.Log("Wanted flags:", []string{
						"$Test" + strconv.Itoa(indx+1) + "-1",
						"$Test" + strconv.Itoa(indx+1) + "-2",
						imap.RecentFlag,
					})
				}
			}

			ch = make(chan *imap.Message, len(expectedSrcRes)+10)
			err = srcMbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchInternalDate, imap.FetchFlags}, ch)
			assert.NilError(t, err)
			assert.Assert(t, is.Len(ch, len(expectedSrcRes)), "Wrong amount of messages left in src mbox")

			for i, indx := range expectedSrcRes {
				msg := <-ch
				assert.Check(t, isNthMsg(msg, indx), "Message #%d left in src mbox is not #%d originally", i+1, indx)
				if !assert.Check(t, isNthMsgFlags(msg, indx), "Message #%d left in src mbox is not #%d originally (flags don't match)", i+1, indx) {
					t.Log("msg.Flags:", msg.Flags)
					t.Log("Wanted flags:", []string{
						"$Test" + strconv.Itoa(indx+1) + "-1",
						"$Test" + strconv.Itoa(indx+1) + "-2",
						imap.RecentFlag,
					})
				}
			}
		})
	}

	t.Run("Non-Existent Dest", func(t *testing.T) {
		srcMbox := getMbox(t, u)
		moveMbox := srcMbox.(move.Mailbox)
		createMsgs(t, srcMbox, 3)
		seq, _ := imap.ParseSeqSet("2:3")
		assert.Error(t, moveMbox.MoveMessages(false, seq, "NONEXISTENT"), backend.ErrNoSuchMailbox.Error())
	})

	cases := []struct {
		uid            bool
		seqset         string
		expectedSrcRes []int
		expectedTgtRes []int
	}{
		{false, "*", []int{}, []int{1, 2, 3}},
		{false, "2:*", []int{1}, []int{2, 3}},
		{false, "1", []int{2, 3}, []int{1}},
		{false, "1,2", []int{3}, []int{1, 2}},
		{false, "1,3", []int{2}, []int{1, 3}},
		{false, "2", []int{1, 3}, []int{2}},
		{false, "1:5", []int{}, []int{1, 2, 3}},
		{false, "1,1:3", []int{}, []int{1, 2, 3}},
		{false, "45:30", []int{1, 2, 3}, []int{}},
	}

	for _, case_ := range cases {
		case_.uid = true
		cases = append(cases, case_)
	}

	if os.Getenv("SHUFFLE_CASES") == "1" {
		rand.Shuffle(len(cases), func(i, j int) {
			cases[i], cases[j] = cases[j], cases[i]
		})
	}

	for _, case_ := range cases {
		testMove(case_.uid, case_.seqset, case_.expectedSrcRes, case_.expectedTgtRes)
	}

	t.Run("Recent flag", func(t *testing.T) {
		srcMbox, tgtMbox := getMbox(t, u), getMbox(t, u)
		createMsgs(t, srcMbox, 1)

		moveMbox := srcMbox.(move.Mailbox)

		seq, err := imap.ParseSeqSet("1")
		if err != nil {
			panic(err)
		}

		assert.NilError(t, srcMbox.UpdateMessagesFlags(false, seq, imap.SetFlags, []string{"$Test1"}))
		assert.NilError(t, moveMbox.MoveMessages(false, seq, tgtMbox.Name()))

		ch := make(chan *imap.Message, 10)
		err = tgtMbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch)
		assert.NilError(t, err)
		assert.Assert(t, is.Len(ch, 1), "Wrong amount of messages copied")

		msg := <-ch
		sort.Strings(msg.Flags)
		assert.Check(t, is.DeepEqual(msg.Flags, []string{"$Test1", imap.RecentFlag}), "Recent flag is not set on moved messages")
	})
}

func Mailbox_MonotonicUid(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())
	mbox := getMbox(t, u)

	createMsgs(t, mbox, 3)

	seq, _ := imap.ParseSeqSet("*")

	var uid uint32
	ch := make(chan *imap.Message, 10)
	err := mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchUid}, ch)
	assert.NilError(t, err)
	msg := <-ch
	uid = msg.Uid
	msg = <-ch
	assert.Check(t, msg.Uid > uid, "UIDs are not increasing")
	uid = msg.Uid
	msg = <-ch
	assert.Check(t, msg.Uid > uid, "UIDs are not increasing")
	uid = msg.Uid

	status, err := mbox.Status([]imap.StatusItem{imap.StatusUidNext})
	assert.NilError(t, err)

	assert.Check(t, status.UidNext > uid, "UIDNEXT is smaller than UID of last message")

	assert.NilError(t, mbox.UpdateMessagesFlags(true, seq, imap.AddFlags, []string{imap.DeletedFlag}))
	assert.NilError(t, mbox.Expunge())

	status2, err := mbox.Status([]imap.StatusItem{imap.StatusUidNext})
	assert.NilError(t, err)

	assert.Equal(t, status2.UidNext, status.UidNext, "EXPUNGE changed UIDNEXT value")
}
