package imapsql

import (
	"bufio"
	"strings"
	"testing"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-message/textproto"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func TestDelivery(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()+"-1", ""), "CreateUser 1")
	assert.NilError(t, b.CreateUser(t.Name()+"-2", ""), "CreateUser 2")

	delivery, err := b.StartDelivery()
	assert.NilError(t, err, "StartDelivery")

	assert.NilError(t, delivery.AddRcpt(t.Name()+"-1", "INBOX"), "AddRcpt 1")
	assert.NilError(t, delivery.AddRcpt(t.Name()+"-2", "INBOX"), "AddRcpt 2")

	assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsgBody)), "BodyRaw")
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

	assert.NilError(t, mbox1.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchEnvelope}, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg := <-ch
	assert.DeepEqual(t, msg.Envelope.From, []*imap.Address{{MailboxName: "foxcpp", HostName: "foxcpp.dev"}})

	hasRecent := false
	for _, flag := range msg.Flags {
		if flag == imap.RecentFlag {
			hasRecent = true
		}
	}
	assert.Assert(t, hasRecent)

	ch = make(chan *imap.Message, 10)
	assert.NilError(t, mbox2.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchEnvelope}, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg = <-ch
	assert.DeepEqual(t, msg.Envelope.From, []*imap.Address{{MailboxName: "foxcpp", HostName: "foxcpp.dev"}})

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
	assert.NilError(t, delivery.AddRcpt(t.Name(), "INBOX"), "AddRcpt")
	assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsgBody)), "BodyRaw")
	assert.NilError(t, delivery.Abort(), "Abort")

	u, err := b.GetUser(t.Name())
	assert.NilError(t, err, "GetUser")
	mbox, err := u.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox")
	status, err := mbox.Status([]imap.StatusItem{imap.StatusMessages})
	assert.NilError(t, err, "mbox.Status")
	assert.Equal(t, status.Messages, uint32(0))
}

func TestDelivery_AddRcpt(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""), "CreateUser")

	delivery, err := b.StartDelivery()
	assert.NilError(t, err, "StartDelivery")
	assert.NilError(t, delivery.AddRcpt(t.Name(), "INBOX"))

	err = delivery.AddRcpt("NON-EXISTENT", "INBOX")
	assert.Assert(t, err != nil, "AddRcpt NON-EXISTENT INBOX")
	err = delivery.AddRcpt(t.Name(), "NON-EXISTENT")
	assert.Assert(t, err != nil, "AddRcpt u1 NON-EXISTENT")

	// Then, however, delivery should continue as if nothing happened.
	assert.NilError(t, delivery.BodyRaw(strings.NewReader(testMsgBody)), "BodyRaw")
	assert.NilError(t, delivery.Commit(), "Commit")

	// Check whether the message is delivered.
	u, err := b.GetUser(t.Name())
	assert.NilError(t, err, "GetUser 1")
	mbox, err := u.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox INBOX")

	seq, _ := imap.ParseSeqSet("*")
	ch := make(chan *imap.Message, 10)

	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchEnvelope}, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg := <-ch
	assert.DeepEqual(t, msg.Envelope.From, []*imap.Address{{MailboxName: "foxcpp", HostName: "foxcpp.dev"}})

	// Below are two subtests that verify whether the the entities created later with non-existent names
	// are not suddenly populated with our message.

	t.Run("NON-EXISTENT mailbox created empty", func(t *testing.T) {
		assert.NilError(t, u.CreateMailbox("NON-EXISTENT"), "CreateMailbox NON-EXISTENT")
		mbox, err := u.GetMailbox("NON-EXISTENT")
		assert.NilError(t, err, "GetMailbox NON-EXISTENT")
		status, err := mbox.Status([]imap.StatusItem{imap.StatusMessages})
		assert.NilError(t, err, "mbox.Status")

		assert.Equal(t, status.Messages, uint32(0), "NON-EXISTENT mailbox non-empty")
	})
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

func TestDelivery_BodyParsed(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name(), ""), "CreateUser")

	delivery, err := b.StartDelivery()
	assert.NilError(t, err, "StartDelivery")

	assert.NilError(t, delivery.AddRcpt(t.Name(), "INBOX"), "AddRcpt")

	hdr, _ := textproto.ReadHeader(bufio.NewReader(strings.NewReader(testMsgBody)))
	assert.NilError(t, delivery.BodyParsed(hdr, strings.NewReader(testMsgBody)), "BodyParsed")
	assert.NilError(t, delivery.Commit(), "Commit")

	u, err := b.GetUser(t.Name())
	assert.NilError(t, err, "GetUser")

	mbox, err := u.GetMailbox("INBOX")
	assert.NilError(t, err, "GetMailbox INBOX")

	seq, _ := imap.ParseSeqSet("*")
	ch := make(chan *imap.Message, 10)

	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchFlags, imap.FetchEnvelope}, ch), "ListMessages")
	assert.Assert(t, is.Len(ch, 1))
	msg := <-ch
	assert.DeepEqual(t, msg.Envelope.From, []*imap.Address{{MailboxName: "foxcpp", HostName: "foxcpp.dev"}})
}
