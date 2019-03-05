package testsuite

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

// Mostly taken from go-imap/backend/backendutil tests.
// https://github.com/emersion/go-imap/blob/v1/backend/backendutil
//
// This tests block is intended to test backend's ability to return
// correct body sections if it implements some advanced indexing
// for them to increase performance.
// go-sqlmail doesn't implement anything like that and just uses backendutil
// for matching so this part of suite is useless for it.

const testBodyString = "--message-boundary\r\n" +
	testAltHeaderString +
	"\r\n--b2\r\n" +
	testTextString +
	"\r\n--b2\r\n" +
	testHTMLString +
	"\r\n--b2--\r\n" +
	"\r\n--message-boundary\r\n" +
	testAttachmentString +
	"\r\n--message-boundary--\r\n"

const testMailString = testHeaderString + testBodyString

var bodyTests = []struct {
	section string
	body    string
}{
	{
		section: "BODY[]",
		body:    testMailString,
	},
	{
		section: "BODY[1.1]",
		body:    testTextBodyString,
	},
	{
		section: "BODY[1.2]",
		body:    testHTMLBodyString,
	},
	{
		section: "BODY[2]",
		body:    testAttachmentBodyString,
	},
	{
		section: "BODY[HEADER]",
		body:    testHeaderString,
	},
	{
		section: "BODY[1.1.HEADER]",
		body:    testTextHeaderString,
	},
	{
		section: "BODY[2.HEADER]",
		body:    testAttachmentHeaderString,
	},
	{
		section: "BODY[2.MIME]",
		body:    testAttachmentHeaderString,
	},
	{
		section: "BODY[TEXT]",
		body:    testBodyString,
	},
	{
		section: "BODY[1.1.TEXT]",
		body:    testTextBodyString,
	},
	{
		section: "BODY[2.TEXT]",
		body:    testAttachmentBodyString,
	},
	{
		section: "BODY[2.1]",
		body:    "",
	},
	{
		section: "BODY[3]",
		body:    "",
	},
	{
		section: "BODY[2.TEXT]<0.9>",
		body:    testAttachmentBodyString[:9],
	},
}

func Mailbox_ListMessages_Body(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
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
	err = mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMailString))
	assert.NilError(t, err)

	seq, _ := imap.ParseSeqSet("1")

	for _, test := range bodyTests {
		test := test
		t.Run(test.section, func(t *testing.T) {
			ch := make(chan *imap.Message, 10)
			assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchItem(test.section)}, ch))
			assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
			msg := <-ch
			assert.Equal(t, msg.SeqNum, uint32(1))

			for k, literal := range msg.Body {
				if k.FetchItem() != imap.FetchItem(test.section) {
					t.Fatal("Unexpected body section returned:", k.FetchItem())
				}

				body, err := ioutil.ReadAll(literal)
				assert.NilError(t, err, "Failed to read body section")

				assert.DeepEqual(t, test.body, string(body))
			}

		})
	}
}

var testBodyStructure = &imap.BodyStructure{
	MIMEType:    "multipart",
	MIMESubType: "mixed",
	Params:      map[string]string{"boundary": "message-boundary"},
	Parts: []*imap.BodyStructure{
		{
			MIMEType:    "multipart",
			MIMESubType: "alternative",
			Params:      map[string]string{"boundary": "b2"},
			Extended:    true,
			Parts: []*imap.BodyStructure{
				{
					MIMEType:          "text",
					MIMESubType:       "plain",
					Params:            map[string]string{},
					Extended:          true,
					Disposition:       "inline",
					DispositionParams: map[string]string{},
				},
				{
					MIMEType:          "text",
					MIMESubType:       "html",
					Params:            map[string]string{},
					Extended:          true,
					Disposition:       "inline",
					DispositionParams: map[string]string{},
				},
			},
		},
		{
			MIMEType:          "text",
			MIMESubType:       "plain",
			Params:            map[string]string{},
			Extended:          true,
			Disposition:       "attachment",
			DispositionParams: map[string]string{"filename": "note.txt"},
		},
	},
	Extended: true,
}

func Mailbox_ListMessages_BodyStruct(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
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
	err = mbox.CreateMessage([]string{"$Test1", "$Test2"}, date, strings.NewReader(testMailString))
	assert.NilError(t, err)

	seq, _ := imap.ParseSeqSet("1")

	ch := make(chan *imap.Message, 10)
	assert.NilError(t, mbox.ListMessages(false, seq, []imap.FetchItem{imap.FetchBodyStructure}, ch))
	assert.Assert(t, is.Len(ch, 1), "Wrong number of messages returned")
	msg := <-ch
	assert.Equal(t, msg.SeqNum, uint32(1))

	assert.DeepEqual(t, msg.BodyStructure, testBodyStructure)
}
