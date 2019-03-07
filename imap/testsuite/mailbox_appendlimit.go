package testsuite

import (
	"strings"
	"testing"
	"time"

	appendlimit "github.com/emersion/go-imap-appendlimit"
	"github.com/emersion/go-imap/backend"
	"github.com/foxcpp/go-sqlmail"
	"gotest.tools/assert"
)

func Backend_AppendLimit(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)

	bAL, ok := b.(sqlmail.AppendLimitBackend)
	if !ok {
		t.Skip("APPENDLIMIT extension is not implemented (need sqlmail.AppendLimitBackend interface)")
		t.SkipNow()
	}

	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	t.Run("No Limit", func(t *testing.T) {
		bAL.SetMessageLimit(0)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 300)))
		assert.NilError(t, err)
	})
	t.Run("Under Limit", func(t *testing.T) {
		bAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 300)))
		assert.NilError(t, err)
	})
	t.Run("Over Limit", func(t *testing.T) {
		bAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 700)))
		assert.Error(t, err, appendlimit.ErrTooBig.Error())
	})
}

func User_AppendLimit(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	bAL, ok := b.(sqlmail.AppendLimitBackend)
	if !ok {
		t.Skip("APPENDLIMIT extension is not implemented (need sqlmail.AppendLimitBackend interface)")
		t.SkipNow()
	}
	uAL, ok := u.(sqlmail.AppendLimitUser)
	if !ok {
		t.Skip("APPENDLIMIT extension is not implemented (need sqlmail.AppendLimitUser interface)")
		t.SkipNow()
	}

	t.Run("No Limit", func(t *testing.T) {
		uAL.SetMessageLimit(0)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 300)))
		assert.NilError(t, err)
	})
	t.Run("Under Limit", func(t *testing.T) {
		uAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 300)))
		assert.NilError(t, err)
	})
	t.Run("Over Limit", func(t *testing.T) {
		uAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 700)))
		assert.Error(t, err, appendlimit.ErrTooBig.Error())
	})
	t.Run("Override backend - Under Limit", func(t *testing.T) {
		bAL.SetMessageLimit(100)
		uAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 400)))
		assert.NilError(t, err)
	})
	t.Run("Override backend - Over Limit", func(t *testing.T) {
		bAL.SetMessageLimit(1000)
		uAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 700)))
		assert.Error(t, err, appendlimit.ErrTooBig.Error())
	})
}

func Mailbox_AppendLimit(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)
	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	bAL, ok := b.(sqlmail.AppendLimitBackend)
	if !ok {
		t.Skip("APPENDLIMIT extension is not implemented (need sqlmail.AppendLimitBackend interface)")
		t.SkipNow()
	}
	uAL, ok := u.(sqlmail.AppendLimitUser)
	if !ok {
		t.Skip("APPENDLIMIT extension is not implemented (need sqlmail.AppendLimitUser interface)")
		t.SkipNow()
	}

	setMboxLim := func(t *testing.T, mbox backend.Mailbox, val uint32) {
		mAL, ok := mbox.(sqlmail.AppendLimitMbox)
		if !ok {
			t.Skip("APPENDLIMIT extension is not implemented (need sqlmail.AppendLimitMbox inteface)")
			t.SkipNow()
		}
		assert.NilError(t, mAL.SetMessageLimit(val))
	}

	t.Run("No Limit", func(t *testing.T) {
		mbox := getMbox(t, u)
		setMboxLim(t, mbox, 500)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 300)))
		assert.NilError(t, err)
	})
	t.Run("Under Limit", func(t *testing.T) {
		mbox := getMbox(t, u)
		setMboxLim(t, mbox, 500)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 300)))
		assert.NilError(t, err)
	})
	t.Run("Over Limit", func(t *testing.T) {
		mbox := getMbox(t, u)
		setMboxLim(t, mbox, 500)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 700)))
		assert.Error(t, err, appendlimit.ErrTooBig.Error())
	})
	t.Run("Override backend - Under Limit", func(t *testing.T) {
		bAL.SetMessageLimit(100)
		mbox := getMbox(t, u)
		setMboxLim(t, mbox, 500)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 400)))
		assert.NilError(t, err)
	})
	t.Run("Override backend - Over Limit", func(t *testing.T) {
		bAL.SetMessageLimit(1000)
		uAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 700)))
		assert.Error(t, err, appendlimit.ErrTooBig.Error())
	})
	t.Run("Override user - Under Limit", func(t *testing.T) {
		uAL.SetMessageLimit(100)
		mbox := getMbox(t, u)
		setMboxLim(t, mbox, 500)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 400)))
		assert.NilError(t, err)
	})
	t.Run("Override user - Over Limit", func(t *testing.T) {
		uAL.SetMessageLimit(1000)
		uAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 700)))
		assert.Error(t, err, appendlimit.ErrTooBig.Error())
	})
	t.Run("Override backend & user - Under Limit", func(t *testing.T) {
		bAL.SetMessageLimit(200)
		uAL.SetMessageLimit(1000)
		uAL.SetMessageLimit(100)
		mbox := getMbox(t, u)
		setMboxLim(t, mbox, 500)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 400)))
		assert.NilError(t, err)
	})
	t.Run("Override backend & user - Over Limit", func(t *testing.T) {
		bAL.SetMessageLimit(2000)
		uAL.SetMessageLimit(1000)
		uAL.SetMessageLimit(500)
		mbox := getMbox(t, u)

		err := mbox.CreateMessage([]string{}, time.Now(), strings.NewReader(strings.Repeat("A", 700)))
		assert.Error(t, err, appendlimit.ErrTooBig.Error())
	})
}
