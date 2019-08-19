package imapsql

import (
	"bufio"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/emersion/go-imap"
	specialuse "github.com/emersion/go-imap-specialuse"
	"github.com/emersion/go-message/textproto"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

var testMsgFetchItems = []imap.FetchItem{imap.FetchEnvelope, imap.FetchFlags, imap.FetchBodyStructure, imap.FetchRFC822Size /*"BODY.PEEK[]",*/, "BODY.PEEK[HEADER]", "BODY.PEEK[TEXT]"}

func checkTestMsg(t *testing.T, msg *imap.Message) {
	for _, item := range msg.Items {
		switch item {
		case imap.FetchEnvelope:
			assert.DeepEqual(t, msg.Envelope, &imap.Envelope{
				Subject: "Hello!",
				From: []*imap.Address{
					{
						MailboxName: "foxcpp",
						HostName:    "foxcpp.dev",
					},
				},
			})
		case imap.FetchFlags:
			assert.DeepEqual(t, msg.Flags, []string{imap.RecentFlag})
		case imap.FetchBodyStructure:
			assert.Equal(t, msg.BodyStructure.MIMEType, "text")
			assert.Equal(t, msg.BodyStructure.MIMESubType, "plain")
		case imap.FetchRFC822Size:
			assert.Equal(t, msg.Size, len(testMsg))
		}
	}

	for key, literal := range msg.Body {
		blob, err := ioutil.ReadAll(literal)
		assert.NilError(t, err, "ReadAll literal")
		switch fetchItem := key.FetchItem(); fetchItem {
		case "BODY.PEEK[]":
			assert.DeepEqual(t, string(blob), testMsg)
		case "BODY.PEEK[HEADER]":
			assert.DeepEqual(t, string(blob), testMsgHeader)
		case "BODY.PEEK[TEXT]":
			assert.DeepEqual(t, string(blob), testMsgBody)
		default:
			t.Log("Unknown part:", fetchItem)
		}
	}
}

func TestDelivery(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()+"-1", ""), "CreateUser 1")
	assert.NilError(t, b.CreateUser(t.Name()+"-2", ""), "CreateUser 2")

	delivery, err := b.StartDelivery()
	assert.NilError(t, err, "StartDelivery")

	assert.NilError(t, delivery.AddRcpt(t.Name()+"-1"), "AddRcpt 1")
	assert.NilError(t, delivery.AddRcpt(t.Name()+"-2"), "AddRcpt 2")

	assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsg)), "BodyRaw")
	assert.NilError(t, delivery.Commit(), "Commit")

	u1, err := b.GetUser(t.Name() + "-1")
	assert.NilError(t, err, "GetUser 1")
	u2, err := b.GetUser(t.Name() + "-2")
	assert.NilError(t, err, "GetUser 2")

	mbox1, err := u1.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox 1 INBOX")
	mbox2, err := u2.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox 2 INBOX")

	seq, _ := imap.ParseSeqSet("*")
	ch := make(chan *imap.Message, 10)

	assert.NilError(t, mbox1.ListMessages(false, seq, testMsgFetchItems, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg := <-ch
	checkTestMsg(t, msg)

	hasRecent := false
	for _, flag := range msg.Flags {
		if flag == imap.RecentFlag {
			hasRecent = true
		}
	}
	assert.Assert(t, hasRecent)

	ch = make(chan *imap.Message, 10)
	assert.NilError(t, mbox2.ListMessages(false, seq, testMsgFetchItems, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg = <-ch
	checkTestMsg(t, msg)

	hasRecent = false
	for _, flag := range msg.Flags {
		if flag == imap.RecentFlag {
			hasRecent = true
		}
	}
	assert.Assert(t, hasRecent)
}

func TestDelivery_Abort(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""), "CreateUser")

	delivery, err := b.StartDelivery()
	assert.NilError(t, err, "StartDelivery")
	assert.NilError(t, delivery.AddRcpt(t.Name()), "AddRcpt")
	assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsg)), "BodyRaw")
	assert.NilError(t, delivery.Abort(), "Abort")

	u, err := b.GetUser(t.Name())
	assert.NilError(t, err, "GetUser")
	mbox, err := u.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox")
	status, err := mbox.Status([]imap.StatusItem{imap.StatusMessages})
	assert.NilError(t, err, "mbox.Status")
	assert.Equal(t, status.Messages, uint32(0))
}

func TestDelivery_AddRcpt_NonExistent(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""), "CreateUser")

	delivery, err := b.StartDelivery()
	assert.NilError(t, err, "StartDelivery")
	assert.NilError(t, delivery.AddRcpt(t.Name()))

	err = delivery.AddRcpt("NON-EXISTENT")
	assert.Assert(t, err != nil, "AddRcpt NON-EXISTENT INBOX")

	// Then, however, delivery should continue as if nothing happened.
	assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsg)), "BodyRaw")
	assert.NilError(t, delivery.Commit(), "Commit")

	// Check whether the message is delivered.
	u, err := b.GetUser(t.Name())
	assert.NilError(t, err, "GetUser 1")
	mbox, err := u.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox INBOX")

	seq, _ := imap.ParseSeqSet("*")
	ch := make(chan *imap.Message, 10)

	assert.NilError(t, mbox.ListMessages(false, seq, testMsgFetchItems, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg := <-ch
	checkTestMsg(t, msg)

	// Below is subtest that verifys whether the the entities created later with non-existent names
	// are not suddenly populated with our message.

	t.Run("NON-EXISTENT user created empty", func(t *testing.T) {
		assert.NilError(t, b.CreateUser("NON-EXISTENT", ""), "CreateUser NON-EXISTENT")
		u, err := b.GetUser("NON-EXISTENT")
		assert.NilError(t, err, "GetUser NON-EXISTENT")
		mbox, err := u.GetMailbox("INBOX")
		assert.NilError(t, err, "GetMailbox INBOX")
		status, err := mbox.Status([]imap.StatusItem{imap.StatusMessages})
		assert.NilError(t, err, "mbox.Status")

		assert.Equal(t, status.Messages, uint32(0), "INBOX of NON-EXISTENT user is non-empty")
	})
}

func TestDelivery_Mailbox(t *testing.T) {
	test := func(t *testing.T, create bool) {
		b := initTestBackend().(*Backend)
		defer cleanBackend(b)
		b.EnableSpecialUseExt()
		assert.NilError(t, b.CreateUser(t.Name(), ""), "CreateUser")
		u, err := b.GetUser(t.Name())
		assert.NilError(t, err, "GetUser")
		if create {
			assert.NilError(t, u.CreateMailbox("Box"))
		}

		delivery, err := b.StartDelivery()
		assert.NilError(t, err, "StartDelivery")

		assert.NilError(t, delivery.AddRcpt(t.Name()), "AddRcpt")

		assert.NilError(t, delivery.Mailbox("Box"))
		assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsg)), "BodyRaw")
		assert.NilError(t, delivery.Commit(), "Commit")

		mbox, err := u.GetMailbox("Box")
		assert.NilError(t, err, "GetMailbox Box")

		seq, _ := imap.ParseSeqSet("*")
		ch := make(chan *imap.Message, 10)

		assert.NilError(t, mbox.ListMessages(false, seq, testMsgFetchItems, ch), "ListMessages")
		assert.Assert(t, is.Len(ch, 1))
		msg := <-ch
		checkTestMsg(t, msg)
	}

	test(t, true)
	t.Run("nonexistent", func(t *testing.T) {
		test(t, false)
	})
}

func TestDelivery_SpecialMailbox(t *testing.T) {
	test := func(t *testing.T, create bool, specialUse string) {
		b := initTestBackend().(*Backend)
		defer cleanBackend(b)
		b.EnableSpecialUseExt()
		assert.NilError(t, b.CreateUser(t.Name(), ""), "CreateUser")
		u, err := b.GetUser(t.Name())
		assert.NilError(t, err, "GetUser")
		if create {
			assert.NilError(t, u.(*User).CreateMailboxSpecial("Box", specialUse))
		}

		delivery, err := b.StartDelivery()
		assert.NilError(t, err, "StartDelivery")

		assert.NilError(t, delivery.AddRcpt(t.Name()), "AddRcpt")

		assert.NilError(t, delivery.SpecialMailbox(specialUse, "Box"))
		assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsg)), "BodyRaw")
		assert.NilError(t, delivery.Commit(), "Commit")

		mbox, err := u.GetMailbox("Box")
		assert.NilError(t, err, "GetMailbox Box")

		seq, _ := imap.ParseSeqSet("*")
		ch := make(chan *imap.Message, 10)

		assert.NilError(t, mbox.ListMessages(false, seq, testMsgFetchItems, ch), "ListMessages")
		assert.Assert(t, is.Len(ch, 1))
		msg := <-ch
		checkTestMsg(t, msg)

		if create {
			info, err := mbox.Info()
			assert.NilError(t, err, "mbox.Info")
			containsSpecial := false
			for _, attr := range info.Attributes {
				if attr == specialUse {
					containsSpecial = true
				}
			}
			assert.Assert(t, containsSpecial, "Missing SPECIAL-USE attr")
		}
	}

	test(t, true, specialuse.Junk)
	t.Run("nonexistent", func(t *testing.T) {
		test(t, false, specialuse.Junk)
	})
}

func TestDelivery_BodyParsed(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""), "CreateUser")

	delivery, err := b.StartDelivery()
	assert.NilError(t, err, "StartDelivery")

	assert.NilError(t, delivery.AddRcpt(t.Name()), "AddRcpt")

	hdr, _ := textproto.ReadHeader(bufio.NewReader(strings.NewReader(testMsgHeader)))
	assert.NilError(t, delivery.BodyParsed(hdr, strings.NewReader(testMsgBody)), "BodyParsed")
	assert.NilError(t, delivery.Commit(), "Commit")

	u, err := b.GetUser(t.Name())
	assert.NilError(t, err, "GetUser")

	mbox, err := u.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox INBOX")

	seq, _ := imap.ParseSeqSet("*")
	ch := make(chan *imap.Message, 10)

	assert.NilError(t, mbox.ListMessages(false, seq, testMsgFetchItems, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg := <-ch
	checkTestMsg(t, msg)
}
