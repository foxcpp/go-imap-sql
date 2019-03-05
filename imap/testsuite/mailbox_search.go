package testsuite

import (
	"net/textproto"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"gotest.tools/assert"
)

// Based on tests for go-imap/backend/backendutil.
// https://github.com/emersion/go-imap/blob/v1/backend/backendutil
//
// Intended for backends using custom search implementation.
// go-sqlmail uses backendutil, so this tests block is useless
// for it.

var matchTests = []struct {
	name     string
	criteria *imap.SearchCriteria
	res      bool
}{
	{
		name: "From hdr",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"From": {"Mitsuha"}},
		},
		res: true,
	},
	{
		name: "To hdr",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"To": {"Mitsuha"}},
		},
		res: false,
	},
	{
		name:     "SentBefore",
		criteria: &imap.SearchCriteria{SentBefore: testDate.Add(48 * time.Hour)},
		res:      true,
	},
	{
		name: "SentSince",
		criteria: &imap.SearchCriteria{
			Not: []*imap.SearchCriteria{{SentSince: testDate.Add(48 * time.Hour)}},
		},
		res: true,
	},
	{
		name: "Body not contains",
		criteria: &imap.SearchCriteria{
			Not: []*imap.SearchCriteria{{Body: []string{"name"}}},
		},
		res: false,
	},
	{
		name: "Text",
		criteria: &imap.SearchCriteria{
			Text: []string{"name"},
		},
		res: true,
	},
	{
		name: "Or",
		criteria: &imap.SearchCriteria{
			Or: [][2]*imap.SearchCriteria{{
				{Text: []string{"i'm not in the text"}},
				{Body: []string{"i'm not in the body"}},
			}},
		},
		res: false,
	},
	{
		name: "Header",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"Message-Id": {"42@example.org"}},
		},
		res: true,
	},
	{
		name: "Header#2",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"Message-Id": {"43@example.org"}},
		},
		res: false,
	},
	{
		name: "Header#3",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"Message-Id": {""}},
		},
		res: true,
	},
	{
		name: "Header#4",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"Reply-To": {""}},
		},
		res: false,
	},
	{
		name: "Size Larger",
		criteria: &imap.SearchCriteria{
			Larger: 10,
		},
		res: true,
	},
	{
		name: "Size Smaller",
		criteria: &imap.SearchCriteria{
			Smaller: 10,
		},
		res: false,
	},
	{
		name: "Header#5",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"Subject": {"your"}},
		},
		res: true,
	},
	{
		name: "Header#6",
		criteria: &imap.SearchCriteria{
			Header: textproto.MIMEHeader{"Subject": {"Taki"}},
		},
		res: false,
	},
}

var flagsTests = []struct {
	flags    []string
	criteria *imap.SearchCriteria
	res      bool
}{
	{
		flags: []string{imap.SeenFlag},
		criteria: &imap.SearchCriteria{
			WithFlags:    []string{imap.SeenFlag},
			WithoutFlags: []string{imap.FlaggedFlag},
		},
		res: true,
	},
	{
		flags: []string{imap.SeenFlag},
		criteria: &imap.SearchCriteria{
			WithFlags:    []string{imap.DraftFlag},
			WithoutFlags: []string{imap.FlaggedFlag},
		},
		res: false,
	},
	{
		flags: []string{imap.SeenFlag, imap.FlaggedFlag},
		criteria: &imap.SearchCriteria{
			WithFlags:    []string{imap.SeenFlag},
			WithoutFlags: []string{imap.FlaggedFlag},
		},
		res: false,
	},
	{
		flags: []string{imap.SeenFlag, imap.FlaggedFlag},
		criteria: &imap.SearchCriteria{
			Or: [][2]*imap.SearchCriteria{{
				{WithFlags: []string{imap.DraftFlag}},
				{WithoutFlags: []string{imap.SeenFlag}},
			}},
		},
		res: false,
	},
	{
		flags: []string{imap.SeenFlag, imap.FlaggedFlag},
		criteria: &imap.SearchCriteria{
			Not: []*imap.SearchCriteria{
				{WithFlags: []string{imap.SeenFlag}},
			},
		},
		res: false,
	},
}

func Mailbox_SearchMessages_Body(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
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

	assert.NilError(t, mbox.CreateMessage([]string{"$Test1", "$Test2"}, testDate, strings.NewReader(testMailString)))

	for _, crit := range matchTests {
		crit := crit
		t.Run("Crit "+crit.name, func(t *testing.T) {
			if crit.name == "SentSince" {
				t.Skip("Skipped due to bug in go-imap (https://github.com/emersion/go-imap/issues/222)")
				t.SkipNow()
			}

			res, err := mbox.SearchMessages(false, crit.criteria)
			assert.NilError(t, err)
			if crit.res {
				assert.Assert(t, len(res) == 1 && res[0] == 1, "Criteria not matched when expected")
			} else {
				assert.Assert(t, len(res) == 0, "Criteria matched when not expected")
			}
		})
	}
}

func Mailbox_SearchMessages_Flags(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	err := b.CreateUser("username1", "password1")
	assert.NilError(t, err)
	u, err := b.GetUser("username1")
	assert.NilError(t, err)
	defer assert.NilError(t, u.Logout())

	for i, crit := range flagsTests {
		crit := crit
		name := "Crit " + strconv.Itoa(i+1)
		t.Run(name, func(t *testing.T) {
			assert.NilError(t, u.CreateMailbox("TEST"+name))
			mbox, err := u.GetMailbox("TEST" + name)
			assert.NilError(t, err)

			assert.NilError(t, mbox.CreateMessage(crit.flags, testDate, strings.NewReader(testMailString)))

			res, err := mbox.SearchMessages(false, crit.criteria)
			assert.NilError(t, err)
			if crit.res {
				assert.Assert(t, len(res) == 1 && res[0] == 1, "Criteria not matched when expected")
			} else {
				assert.Assert(t, len(res) == 0, "Criteria matched when not expected")
			}
		})
	}
}
