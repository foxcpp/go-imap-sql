package imapsql

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-message/textproto"
	"gotest.tools/assert"
)

const malformedMIMEMessage = "From: sender@example.org\r\n" +
	"Subject: malformed\r\n" +
	"Content-Type: multipart/mixed; boundary=boundary\r\n" +
	"\r\n" +
	"--boundary\r\n" +
	"<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"\r\n" +
	"<html>body</html>\r\n" +
	"--boundary--\r\n"

const malformedTopLevelMessage = "not a MIME header\r\n\r\nraw message\r\n"

func TestCreateMessageMalformedMIME(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)

	assert.NilError(t, usr.CreateMessage("INBOX", nil, time.Now(),
		strings.NewReader(malformedMIMEMessage), nil))

	_, mailbox, err := usr.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	mbox := mailbox.(*Mailbox)
	defer mbox.Close()
	var storedBodyStructure string
	assert.NilError(t, b.DB.QueryRow(
		"SELECT bodyStructure FROM msgs WHERE mboxId = ? AND msgId = ?",
		mbox.id, 1,
	).Scan(&storedBodyStructure))
	assert.Assert(t, strings.Contains(storedBodyStructure, `"MalformedMIME":true`))

	seq, err := imap.ParseSeqSet("1")
	assert.NilError(t, err)
	ch := make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq,
		[]imap.FetchItem{imap.FetchBody, imap.FetchBodyStructure, "BODY.PEEK[]", "BODY.PEEK[HEADER]", "BODY.PEEK[TEXT]", "BODY.PEEK[1]", "BODY.PEEK[1]<2.5>"}, ch))

	msg := <-ch
	assert.Equal(t, msg.BodyStructure.MIMEType, "application")
	assert.Equal(t, msg.BodyStructure.MIMESubType, "octet-stream")

	var body []byte
	var header []byte
	var text []byte
	var part []byte
	var partial []byte
	for section, literal := range msg.Body {
		switch section.FetchItem() {
		case "BODY.PEEK[]":
			body, err = ioutil.ReadAll(literal)
			assert.NilError(t, err)
		case "BODY.PEEK[HEADER]":
			header, err = ioutil.ReadAll(literal)
			assert.NilError(t, err)
		case "BODY.PEEK[TEXT]":
			text, err = ioutil.ReadAll(literal)
			assert.NilError(t, err)
		case "BODY.PEEK[1]":
			part, err = ioutil.ReadAll(literal)
			assert.NilError(t, err)
		case "BODY.PEEK[1]<2.5>":
			partial, err = ioutil.ReadAll(literal)
			assert.NilError(t, err)
		}
	}
	assert.Equal(t, string(body), malformedMIMEMessage)
	assert.Equal(t, string(header), "From: sender@example.org\r\nSubject: malformed\r\nContent-Type: multipart/mixed; boundary=boundary\r\n\r\n")
	assert.Equal(t, string(text), "--boundary\r\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"\r\n<html>body</html>\r\n--boundary--\r\n")
	assert.Equal(t, string(part), "--boundary\r\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"\r\n<html>body</html>\r\n--boundary--\r\n")
	assert.Equal(t, string(partial), "bound")
}

func TestDeliveryMalformedMIME(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()))

	delivery := b.NewDelivery()
	assert.NilError(t, delivery.AddRcpt(t.Name(), textproto.Header{}))
	assert.NilError(t, delivery.BodyRaw(strings.NewReader(malformedMIMEMessage)))
	assert.NilError(t, delivery.Commit())

	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	_, mbox, err := usr.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	defer mbox.Close()

	seq, err := imap.ParseSeqSet("1")
	assert.NilError(t, err)
	ch := make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq,
		[]imap.FetchItem{imap.FetchBodyStructure}, ch))

	msg := <-ch
	assert.Equal(t, msg.BodyStructure.MIMEType, "application")
	assert.Equal(t, msg.BodyStructure.MIMESubType, "octet-stream")
}

func TestCreateMessageMalformedTopLevelHeader(t *testing.T) {
	b := initTestBackend()
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)

	assert.NilError(t, usr.CreateMessage("INBOX", nil, time.Now(),
		strings.NewReader(malformedTopLevelMessage), nil))

	_, mailbox, err := usr.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	mbox := mailbox.(*Mailbox)
	defer mbox.Close()

	seq, err := imap.ParseSeqSet("1")
	assert.NilError(t, err)
	ch := make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq,
		[]imap.FetchItem{imap.FetchBodyStructure, "BODY.PEEK[]"}, ch))

	msg := <-ch
	assert.Equal(t, msg.BodyStructure.MIMEType, "application")
	assert.Equal(t, msg.BodyStructure.MIMESubType, "octet-stream")

	for section, literal := range msg.Body {
		if section.FetchItem() != "BODY.PEEK[]" {
			continue
		}
		body, err := ioutil.ReadAll(literal)
		assert.NilError(t, err)
		assert.Equal(t, string(body), malformedTopLevelMessage)
	}
}

func TestMalformedMIMEMismatchedBodySelectors(t *testing.T) {
	b := initTestBackend().(*Backend)
	defer cleanBackend(b)
	assert.NilError(t, b.CreateUser(t.Name()))
	usr, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	assert.NilError(t, usr.CreateMessage("INBOX", nil, time.Now(),
		strings.NewReader(malformedMIMEMessage), nil))

	_, mailbox, err := usr.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	mbox := mailbox.(*Mailbox)
	defer mbox.Close()

	seq, err := imap.ParseSeqSet("1")
	assert.NilError(t, err)
	ch := make(chan *imap.Message, 1)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{
		"BODY.PEEK[1.1]",
		"BODY.PEEK[1.1.1]",
		"BODY.PEEK[2]",
	}, ch))

	msg := <-ch
	assert.Assert(t, msg != nil)
	for section, literal := range msg.Body {
		body, err := ioutil.ReadAll(literal)
		assert.NilError(t, err)
		assert.Equal(t, len(body), 0, section.FetchItem())
	}
}
