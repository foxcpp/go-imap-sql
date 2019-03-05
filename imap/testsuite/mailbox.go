package testsuite

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func Mailbox_Info(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
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
		t.Skip("CHILDREN extension is not implemeted yet")
		t.SkipNow()

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

func Mailbox_Status(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	t.Run("UidNext", func(t *testing.T) {
		assert.NilError(t, u.CreateMailbox("TEST"))
		mbox, err := u.GetMailbox("TEST")
		assert.NilError(t, err)

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
		assert.NilError(t, u.CreateMailbox("TEST2"))
		mbox, err := u.GetMailbox("TEST2")
		assert.NilError(t, err)

		date := time.Now()
		err = mbox.CreateMessage([]string{"$Test3", "$Test4"}, date, strings.NewReader(testMsg))
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
		assert.NilError(t, u.CreateMailbox("TEST3"))
		mbox, err := u.GetMailbox("TEST3")
		assert.NilError(t, err)

		date := time.Now()
		err = mbox.CreateMessage([]string{"$Test3", "$Test4", imap.SeenFlag}, date, strings.NewReader(testMsg))
		assert.NilError(t, err)

		date = time.Now()
		err = mbox.CreateMessage([]string{"$Test3", "$Test4"}, date, strings.NewReader(testMsg))
		assert.NilError(t, err)

		status, err := mbox.Status(nil)
		assert.NilError(t, err)
		assert.Equal(t, status.UnseenSeqNum, uint32(2), "UnseenSeqNum is invalid")
	})
}

func Mailbox_SetSubscribed(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST"))
	mbox, err := u.GetMailbox("TEST")
	assert.NilError(t, err)

	// Initial state of subscription is undefined, so don't check it.

	t.Run("SetSubscribed true", func(t *testing.T) {
		assert.NilError(t, mbox.SetSubscribed(true))
		mboxes, err := u.ListMailboxes(true)
		assert.NilError(t, err)

		assert.Assert(t, len(mboxes) == 1 && mboxes[0].Name() == "TEST", "Mailbox is not present in list when subscribed")
	})
	t.Run("SetSubscribed false", func(t *testing.T) {
		assert.NilError(t, mbox.SetSubscribed(false))
		mboxes, err := u.ListMailboxes(true)
		assert.NilError(t, err)

		assert.Assert(t, len(mboxes) == 0, "Mailbox is present in list when unsubscribed")
	})
}

func Mailbox_CreateMessage(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST"))
	mbox, err := u.GetMailbox("TEST")
	assert.NilError(t, err)

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

	assert.Assert(t, msg.InternalDate.Truncate(time.Second).Equal(date.Truncate(time.Second)), "InternalDate is not same")
	assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
	assert.Equal(t, uint32(len(testMsg)), msg.Size, "RFC822 size mismatch")
	for _, v := range msg.Body {
		b, err := ioutil.ReadAll(v)
		assert.NilError(t, err, "ReadAll body failed")
		assert.DeepEqual(t, testMsg, b)
	}
}

func Mailbox_ListMessages(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST"))
	mbox, err := u.GetMailbox("TEST")
	assert.NilError(t, err)
	date := time.Now()
	body := `To: test@test
From: test <test@test>
Subject: test
Date: Tue, 8 May 2018 20:48:21 +0000
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: 7bit
Cc: foo <foo@foo>, bar <bar@bar>
X-CustomHeader: foo

Test! Test! Test! Test!
`

	err = mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(body))
	assert.NilError(t, err)

	err = mbox.CreateMessage([]string{"$Test3", "$Test4"}, date, strings.NewReader(body))
	assert.NilError(t, err)

	err = mbox.CreateMessage([]string{}, date, strings.NewReader(body))
	assert.NilError(t, err)

	firstUid, secondUid, thirdUid := uint32(0), uint32(0), uint32(0)

	if !t.Run("Seq1:3", func(t *testing.T) {
		seq, _ := imap.ParseSeqSet("1:3")
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 3), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(1))
		firstUid = msg.Uid
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(2))
		secondUid = msg.Uid
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(3))
		thirdUid = msg.Uid
	}) {
		t.FailNow()
	}

	// Invalid seqnums should be ignored.
	t.Run("Seq1:5 (4,5 invalid)", func(t *testing.T) {
		seq, _ := imap.ParseSeqSet("1:5")
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 3), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(1))
		assert.Equal(t, msg.Uid, firstUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(2))
		assert.Equal(t, msg.Uid, secondUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(3))
		assert.Equal(t, msg.Uid, thirdUid)
	})

	// Return all messages.
	t.Run("Seq1:*", func(t *testing.T) {
		seq, _ := imap.ParseSeqSet("1:*")
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 3), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(1))
		assert.Equal(t, msg.Uid, firstUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(2))
		assert.Equal(t, msg.Uid, secondUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(3))
		assert.Equal(t, msg.Uid, thirdUid)
	})

	// Return only one message.
	t.Run("Seq1", func(t *testing.T) {
		seq, _ := imap.ParseSeqSet("1")
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(1))
		assert.Equal(t, msg.Uid, firstUid)
	})

	t.Run("Seq25:30 (fully invalid)", func(t *testing.T) {
		seq, _ := imap.ParseSeqSet("25:30")
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 0), "Wrong number of messages returned")
	})

	// Return all messages.
	t.Run("Uid1:*", func(t *testing.T) {
		seq, _ := imap.ParseSeqSet("1:*")
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 3), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(1))
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(2))
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(3))
	})

	// Return first two messages using UID.
	t.Run("Uid1,2", func(t *testing.T) {
		seq := imap.SeqSet{}
		seq.AddNum(firstUid)
		seq.AddNum(secondUid)
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(true, &seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 2), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(1))
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(2))
	})

	// Return all three messages using UID range.
	t.Run("Uid1:3", func(t *testing.T) {
		seq := imap.SeqSet{}
		seq.AddRange(firstUid, thirdUid)
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(true, &seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 3), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(1))
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(2))
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		assert.Equal(t, msg.SeqNum, uint32(3))
	})

	// Return all three messages using UID range with invalid entires.
	t.Run("Uid1:5", func(t *testing.T) {
		seq := imap.SeqSet{}
		seq.AddRange(firstUid, thirdUid+2)
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(true, &seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 3), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.Uid, firstUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.Uid, secondUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		assert.Equal(t, msg.Uid, thirdUid)
	})

	t.Run("Uid51:53 (fully invalid)", func(t *testing.T) {
		seq := imap.SeqSet{}
		seq.AddRange(firstUid+50, thirdUid+50)
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(true, &seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 0), "Wrong number of messages returned")
	})

	t.Run("Uid51:53,1:3", func(t *testing.T) {
		seq := imap.SeqSet{}
		seq.AddRange(firstUid+50, thirdUid+50)
		seq.AddRange(firstUid, thirdUid)
		ch := make(chan *imap.Message, 10)
		assert.NilError(t, mbox.ListMessages(true, &seq, []imap.FetchItem{imap.FetchFlags, imap.FetchUid}, ch))
		assert.Assert(t, is.Len(ch, 3), "Wrong number of messages returned")
		msg := <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag})
		assert.Equal(t, msg.Uid, firstUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag})
		assert.Equal(t, msg.Uid, secondUid)
		msg = <-ch
		assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		assert.Equal(t, msg.Uid, thirdUid)
	})
}

func Mailbox_SetMessageFlags(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	t.Run("AddFlags", func(t *testing.T) {
		t.Run("Uid", func(t *testing.T) {
			assert.NilError(t, u.CreateMailbox("TEST"))
			mbox, err := u.GetMailbox("TEST")
			assert.NilError(t, err)
			seq, _ := imap.ParseSeqSet("1")
			ch := make(chan *imap.Message, 10)

			date := time.Now()
			assert.NilError(t, mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg)))
			assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchUid}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg := <-ch
			seq = &imap.SeqSet{}
			seq.AddNum(msg.Uid)

			assert.NilError(t, mbox.UpdateMessagesFlags(true, seq, imap.AddFlags, []string{"$Test3"}))

			ch = make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg = <-ch

			assert.DeepEqual(t, []string{"$Test1", "$Test2", "$Test3", imap.RecentFlag}, msg.Flags)
		})
		t.Run("Seq", func(t *testing.T) {
			assert.NilError(t, u.CreateMailbox("TESTS"))
			mbox, err := u.GetMailbox("TESTS")
			assert.NilError(t, err)
			seq, _ := imap.ParseSeqSet("1")

			date := time.Now()
			assert.NilError(t, mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg)))

			assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.AddFlags, []string{"$Test3"}))

			ch := make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg := <-ch

			assert.DeepEqual(t, []string{"$Test1", "$Test2", "$Test3", imap.RecentFlag}, msg.Flags)
		})
	})

	t.Run("RemoveFlags", func(t *testing.T) {
		t.Run("Uid", func(t *testing.T) {
			assert.NilError(t, u.CreateMailbox("TEST2"))
			mbox, err := u.GetMailbox("TEST2")
			assert.NilError(t, err)
			seq, _ := imap.ParseSeqSet("1")
			ch := make(chan *imap.Message, 10)

			date := time.Now()
			assert.NilError(t, mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg)))
			assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchUid}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg := <-ch
			seq = &imap.SeqSet{}
			seq.AddNum(msg.Uid)

			assert.NilError(t, mbox.UpdateMessagesFlags(true, seq, imap.RemoveFlags, []string{"$Test2"}))

			ch = make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg = <-ch

			assert.DeepEqual(t, []string{"$Test1", imap.RecentFlag}, msg.Flags)

			t.Run("repeat", func(t *testing.T) {
				assert.NilError(t, mbox.UpdateMessagesFlags(true, seq, imap.RemoveFlags, []string{"$Test2"}))

				ch := make(chan *imap.Message, 10)
				assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchFlags}, ch))
				assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
				msg = <-ch

				assert.DeepEqual(t, []string{"$Test1", imap.RecentFlag}, msg.Flags)
			})
		})
		t.Run("Seq", func(t *testing.T) {
			assert.NilError(t, u.CreateMailbox("TEST2S"))
			mbox, err := u.GetMailbox("TEST2S")
			assert.NilError(t, err)
			seq, _ := imap.ParseSeqSet("1")

			date := time.Now()
			assert.NilError(t, mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg)))
			assert.NilError(t, err)

			assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.RemoveFlags, []string{"$Test2"}))

			ch := make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg := <-ch

			assert.DeepEqual(t, []string{"$Test1", imap.RecentFlag}, msg.Flags)

			t.Run("repeat", func(t *testing.T) {
				assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.RemoveFlags, []string{"$Test2"}))

				ch := make(chan *imap.Message, 10)
				assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch))
				assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
				msg := <-ch

				assert.DeepEqual(t, []string{"$Test1", imap.RecentFlag}, msg.Flags)
			})
		})
	})

	t.Run("SetFlags", func(t *testing.T) {
		t.Run("Uid", func(t *testing.T) {
			assert.NilError(t, u.CreateMailbox("TEST3"))
			mbox, err := u.GetMailbox("TEST3")
			assert.NilError(t, err)
			seq, _ := imap.ParseSeqSet("1")
			ch := make(chan *imap.Message, 10)

			date := time.Now()
			assert.NilError(t, mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg)))
			assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchUid}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg := <-ch
			seq = &imap.SeqSet{}
			seq.AddNum(msg.Uid)

			assert.NilError(t, mbox.UpdateMessagesFlags(true, seq, imap.SetFlags, []string{"$Test3", "$Test4"}))

			ch = make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg = <-ch

			assert.DeepEqual(t, []string{"$Test3", "$Test4"}, msg.Flags)

			t.Run("repeat", func(t *testing.T) {
				assert.NilError(t, mbox.UpdateMessagesFlags(true, seq, imap.SetFlags, []string{"$Test3", "$Test4"}))

				ch := make(chan *imap.Message, 10)
				assert.NilError(t, mbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchFlags}, ch))
				assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
				msg = <-ch

				assert.DeepEqual(t, []string{"$Test3", "$Test4"}, msg.Flags)
			})
		})
		t.Run("Seq", func(t *testing.T) {
			assert.NilError(t, u.CreateMailbox("TEST3S"))
			mbox, err := u.GetMailbox("TEST3S")
			assert.NilError(t, err)
			seq, _ := imap.ParseSeqSet("1")

			date := time.Now()
			assert.NilError(t, mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg)))
			assert.NilError(t, err)

			assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.SetFlags, []string{"$Test3", "$Test4"}))

			ch := make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg := <-ch

			assert.DeepEqual(t, []string{"$Test3", "$Test4"}, msg.Flags)

			t.Run("repeat", func(t *testing.T) {
				assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.SetFlags, []string{"$Test3", "$Test4"}))

				ch := make(chan *imap.Message, 10)
				assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch))
				assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
				msg = <-ch

				assert.DeepEqual(t, []string{"$Test3", "$Test4"}, msg.Flags)
			})
		})
	})
}

func Mailbox_Expunge(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())
	assert.NilError(t, u.CreateMailbox("TEST4"))
	mbox, err := u.GetMailbox("TEST4")
	assert.NilError(t, err)

	date := time.Now()
	err = mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMsg))
	assert.NilError(t, err)
	err = mbox.CreateMessage([]string{"$Test3", "$Test4"}, date, strings.NewReader(testMsg))
	assert.NilError(t, err)
	err = mbox.CreateMessage([]string{"$Test5", "$Test6"}, date, strings.NewReader(testMsg))
	assert.NilError(t, err)

	assert.NilError(t, mbox.Expunge())

	status, err := mbox.Status([]imap.StatusItem{imap.StatusMessages})
	assert.NilError(t, err)
	assert.Equal(t, status.Messages, uint32(3), "Expunge deleted non-flagged messages")

	seq, _ := imap.ParseSeqSet("2:3")
	assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.AddFlags, []string{imap.DeletedFlag}))

	assert.NilError(t, mbox.Expunge())

	seq, _ = imap.ParseSeqSet("*:*")
	ch := make(chan *imap.Message, 10)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags}, ch))
	assert.Assert(t, is.Len(ch, 1), "Expunge didn't removed messages or removed more of them")
	msg := <-ch

	assert.Assert(t, is.DeepEqual(msg.Flags, []string{"$Test1", "$Test2", imap.RecentFlag}), "Wrong messages deleted")
}

func Mailbox_CopyMessages(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	assert.NilError(t, u.CreateMailbox("TEST"))
	mbox, err := u.GetMailbox("TEST")
	assert.NilError(t, err)

	date := time.Now()
	err = mbox.CreateMessage([]string{"$Test1", "$Test2"}, date.Add(24*time.Hour), strings.NewReader(testMsg))
	assert.NilError(t, err)
	err = mbox.CreateMessage([]string{"$Test3", "$Test4"}, date.Add(48*time.Hour), strings.NewReader(testMsg))
	assert.NilError(t, err)
	err = mbox.CreateMessage([]string{"$Test5", "$Test6"}, date.Add(72*time.Hour), strings.NewReader(testMsg))
	assert.NilError(t, err)

	seq, _ := imap.ParseSeqSet("1:3")

	ch := make(chan *imap.Message, 10)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchUid}, ch))
	assert.Assert(t, is.Len(ch, 3))
	<-ch
	msg := <-ch
	secondUid := msg.Uid
	msg = <-ch
	thirdUid := msg.Uid

	date = date.Truncate(time.Second)

	t.Run("Seq", func(t *testing.T) {
		assert.NilError(t, u.CreateMailbox("TEST2"))
		tmbox, err := u.GetMailbox("TEST2")
		assert.NilError(t, err)

		seq, _ := imap.ParseSeqSet("2:3")
		assert.NilError(t, mbox.CopyMessages(false, seq, "TEST2"))

		ch := make(chan *imap.Message, 10)
		seq, _ = imap.ParseSeqSet("*")
		assert.NilError(t, tmbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchInternalDate}, ch))
		assert.Assert(t, is.Len(ch, 2), "Extra or no messages created in dest mailbox")
		msg := <-ch
		assert.Check(t, is.DeepEqual(msg.InternalDate, date.Add(48*time.Hour)), "Messages copied in wrong order")
		assert.Check(t, is.DeepEqual(msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag}), "Flags are not copied")
		msg = <-ch
		assert.Check(t, is.DeepEqual(msg.InternalDate, date.Add(72*time.Hour)), "Messages copied in wrong order")
		assert.Check(t, is.DeepEqual(msg.Flags, []string{"$Test5", "$Test6", imap.RecentFlag}), "Flags are not copied")
	})
	t.Run("Uid", func(t *testing.T) {
		assert.NilError(t, u.CreateMailbox("TEST3"))
		tmbox, err := u.GetMailbox("TEST3")
		assert.NilError(t, err)

		seq := &imap.SeqSet{}
		seq.AddNum(secondUid, thirdUid)
		assert.NilError(t, mbox.CopyMessages(true, seq, "TEST3"))

		ch := make(chan *imap.Message, 10)
		seq, _ = imap.ParseSeqSet("*")
		assert.NilError(t, tmbox.ListMessages(true, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchInternalDate}, ch))
		assert.Assert(t, is.Len(ch, 2), "Extra or no messages created in dest mailbox")
		msg := <-ch
		assert.Check(t, is.DeepEqual(msg.InternalDate, date.Add(48*time.Hour)), "Messages copied in wrong order")
		assert.Check(t, is.DeepEqual(msg.Flags, []string{"$Test3", "$Test4", imap.RecentFlag}), "Flags are not copied")
		msg = <-ch
		assert.Check(t, is.DeepEqual(msg.InternalDate, date.Add(72*time.Hour)), "Messages copied in wrong order")
		assert.Check(t, is.DeepEqual(msg.Flags, []string{"$Test5", "$Test6", imap.RecentFlag}), "Flags are not copied")
	})
}
