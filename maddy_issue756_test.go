package imapsql

import (
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"gotest.tools/assert"
)

func TestFetchBodyStructureDefaultsEncodingTo7Bit(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)

	assert.NilError(t, b.CreateUser(t.Name()))
	u, err := b.GetUser(t.Name())
	assert.NilError(t, err)

	message := "From: sender@example.com\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"hello\r\n"
	assert.NilError(t, u.CreateMessage("INBOX", nil, time.Now(), strings.NewReader(message), nil))

	_, mboxRaw, err := u.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	mbox := mboxRaw.(*Mailbox)
	defer mbox.Close()

	seq, _ := imap.ParseSeqSet("1")
	ch := make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchBodyStructure}, ch))

	fetched := <-ch
	assert.Assert(t, fetched.BodyStructure != nil)
	assert.Equal(t, fetched.BodyStructure.Encoding, "7BIT")
}

func TestFetchBodyStructureBackfillsEncodingForLegacyMessage(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)

	assert.NilError(t, b.CreateUser(t.Name()))
	u, err := b.GetUser(t.Name())
	assert.NilError(t, err)

	message := "From: sender@example.com\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"hello\r\n"
	assert.NilError(t, u.CreateMessage("INBOX", nil, time.Now(), strings.NewReader(message), nil))

	_, mboxRaw, err := u.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	mbox := mboxRaw.(*Mailbox)
	defer mbox.Close()

	var bodyStructure string
	assert.NilError(t, b.db.QueryRow(
		"SELECT bodyStructure FROM msgs WHERE mboxId = ? AND msgId = ?",
		mbox.id, 1,
	).Scan(&bodyStructure))
	bodyStructure = strings.Replace(bodyStructure, `"Encoding":"7BIT"`, `"Encoding":""`, 1)
	_, err = b.db.Exec(
		"UPDATE msgs SET bodyStructure = ? WHERE mboxId = ? AND msgId = ?",
		bodyStructure, mbox.id, 1,
	)
	assert.NilError(t, err)

	seq, _ := imap.ParseSeqSet("1")
	ch := make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchBodyStructure}, ch))

	fetched := <-ch
	assert.Assert(t, fetched.BodyStructure != nil)
	assert.Equal(t, fetched.BodyStructure.Encoding, "7BIT")
}
