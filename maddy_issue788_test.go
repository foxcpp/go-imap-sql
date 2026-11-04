package imapsql

import (
	"net"
	"strings"
	"testing"

	"github.com/emersion/go-imap"
	imapclient "github.com/emersion/go-imap/client"
	imapserver "github.com/emersion/go-imap/server"
	"gotest.tools/assert"
)

func TestStatusAppendLimitReflectsEffectiveLimit(t *testing.T) {
	b := setupTestBackend(t)

	assert.NilError(t, b.CreateUser(t.Name()))
	usrRaw, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	usr := usrRaw.(*User)

	_, mboxRaw, err := usr.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	mbox := mboxRaw.(*Mailbox)
	defer mbox.Close()

	statusLimit := func() uint32 {
		t.Helper()

		status, err := usr.Status("INBOX", []imap.StatusItem{imap.StatusAppendLimit})
		assert.NilError(t, err)

		return status.AppendLimit
	}

	backendLimit := uint32(1000)
	assert.NilError(t, b.SetMessageLimit(&backendLimit))
	assert.Equal(t, statusLimit(), backendLimit, "backend limit must be reported")

	userLimit := uint32(500)
	assert.NilError(t, usr.SetMessageLimit(&userLimit))
	assert.Equal(t, statusLimit(), userLimit, "user limit must override backend limit")

	mboxLimit := uint32(200)
	assert.NilError(t, mbox.SetMessageLimit(&mboxLimit))
	assert.Equal(t, statusLimit(), mboxLimit, "mailbox limit must override user limit")

	assert.NilError(t, mbox.SetMessageLimit(nil))
	assert.Equal(t, statusLimit(), userLimit, "mailbox limit must fall back to user limit")

	assert.NilError(t, usr.SetMessageLimit(nil))
	assert.Equal(t, statusLimit(), backendLimit, "user limit must fall back to backend limit")
}

// TestAppendLimitCapabilityStaysBare guards the capability form chosen by the
// server.
//
// RFC 7889 section 2 gives the two forms different meanings: "APPENDLIMIT=<n>"
// asserts that the limit is the same for all mailboxes, while the bare
// "APPENDLIMIT" capability tells the client to discover the limit per mailbox
// with STATUS. This backend supports per-mailbox limits, so a numbered
// capability would be a false claim once a mailbox limit differs from the
// advertised number: a client trusting CAPABILITY would skip STATUS entirely
// and try to upload messages the server then rejects with TOOBIG.
//
// In particular User.CreateMessageLimit must keep reporting the per-user
// setting instead of the effective limit, otherwise the limit of the
// connection's INBOX leaks into the capability.
func TestAppendLimitCapabilityStaysBare(t *testing.T) {
	b := setupTestBackend(t)

	// maddy's default "appendlimit" value, so this test runs the configuration
	// from the bug report.
	backendLimit := uint32(32 * 1024 * 1024)
	assert.NilError(t, b.SetMessageLimit(&backendLimit))
	assert.NilError(t, b.CreateUser(t.Name()))

	c := dialTestServer(t, b.Backend, t.Name())
	t.Cleanup(func() { c.Logout() })

	assertBareCap := func(when string) {
		t.Helper()

		caps, err := c.Capability()
		assert.NilError(t, err)

		for cap := range caps {
			assert.Assert(t, !strings.HasPrefix(cap, "APPENDLIMIT="),
				"%s: APPENDLIMIT must not be advertised with a value (got %q), the limit is per-mailbox", when, cap)
		}
		assert.Assert(t, caps["APPENDLIMIT"],
			"%s: APPENDLIMIT capability must still be advertised", when)
	}

	statusLimit := func() uint32 {
		t.Helper()

		status, err := c.Status("INBOX", []imap.StatusItem{imap.StatusAppendLimit})
		assert.NilError(t, err)

		return status.AppendLimit
	}

	assertBareCap("no overrides")
	assert.Equal(t, statusLimit(), backendLimit, "STATUS must report the backend-wide limit")

	// Make the mailbox limit differ from the backend-wide one. A numbered
	// capability would now be wrong for at least one of the two values.
	usrRaw, err := b.GetUser(t.Name())
	assert.NilError(t, err)
	usr := usrRaw.(*User)
	_, mboxRaw, err := usr.GetMailbox("INBOX", true, &noopConn{})
	assert.NilError(t, err)
	mbox := mboxRaw.(*Mailbox)
	mboxLimit := uint32(1000)
	assert.NilError(t, mbox.SetMessageLimit(&mboxLimit))
	assert.NilError(t, mbox.Close())

	assertBareCap("per-mailbox override")
	assert.Equal(t, statusLimit(), mboxLimit, "STATUS must report the per-mailbox limit")
}

func dialTestServer(t *testing.T, b *Backend, username string) *imapclient.Client {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NilError(t, err)

	srv := imapserver.New(b)
	srv.AllowInsecureAuth = true

	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = srv.Serve(l)
	}()
	t.Cleanup(func() {
		srv.Close()
		<-served
	})

	c, err := imapclient.Dial(l.Addr().String())
	assert.NilError(t, err)

	// The backend does not authenticate passwords, they are checked by the
	// server integration in front of it.
	assert.NilError(t, c.Login(username, ""))

	return c
}
