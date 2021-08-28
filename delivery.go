package imapsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"io"
	"io/ioutil"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-message/textproto"
)

var ErrDeliveryInterrupted = errors.New("sql: delivery transaction interrupted, try again later")

// NewDelivery creates a new state object for atomic delivery session.
//
// Messages added to the storage using that interface are added either to
// all recipients mailboxes or none or them.
//
// Also use of this interface is more efficient than separate GetUser/GetMailbox/CreateMessage
// calls.
//
// Note that for performance reasons, the DB is not locked while the Delivery object
// exists, but only when BodyRaw/BodyParsed is called and until Abort/Commit is called.
// This means that the recipient mailbox can be deleted between AddRcpt and Body* calls.
// In that case, either Body* or Commit will return ErrDeliveryInterrupt.
// Sender should retry delivery after a short delay.
func (b *Backend) NewDelivery() Delivery {
	return Delivery{b: b, perRcptHeader: map[string]textproto.Header{}}
}

func (d *Delivery) clean() {
	d.users = d.users[0:0]
	d.mboxes = d.mboxes[0:0]
	d.updates = d.updates[0:0]
	d.extKey = ""
	for k := range d.perRcptHeader {
		delete(d.perRcptHeader, k)
	}
}

type Delivery struct {
	b             *Backend
	tx            *sql.Tx
	users         []User
	mboxes        []Mailbox
	extKey        string
	updates       []backend.Update
	perRcptHeader map[string]textproto.Header
	flagOverrides map[string][]string
	mboxOverrides map[string]string
}

// AddRcpt adds the recipient username/mailbox pair to the delivery.
//
// If this function returns an error - further calls will still work
// correctly and there is no need to restart the delivery.
//
// The specified user account and mailbox should exist at the time AddRcpt
// is called, but it can disappear before Body* call, in which case
// Delivery will be terminated with ErrDeliveryInterrupted error.
// See Backend.StartDelivery method documentation for details.
//
// Fields from userHeader, if any, will be prepended to the message header
// *only* for that recipient. Use this to add Received and Delivered-To
// fields with recipient-specific information (e.g. its address).
func (d *Delivery) AddRcpt(username string, userHeader textproto.Header) error {
	username = normalizeUsername(username)

	uid, inboxId, err := d.b.getUserMeta(nil, username)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrUserDoesntExists
		}
		return err
	}
	d.users = append(d.users, User{id: uid, username: username, parent: d.b, inboxId: inboxId})

	d.perRcptHeader[username] = userHeader

	return nil
}

// FIXME: Fix that goddamned code duplication.

// Mailbox command changes the target mailbox for all recipients.
// It should be called before BodyParsed/BodyRaw.
//
// If it is not called, it defaults to INBOX. If mailbox doesn't
// exist for some users - it will created.
func (d *Delivery) Mailbox(name string) error {
	if cap(d.mboxes) < len(d.users) {
		d.mboxes = make([]Mailbox, 0, len(d.users))
	}

	for _, u := range d.users {
		if mboxName := d.mboxOverrides[u.username]; mboxName != "" {
			mbox, err := u.GetMailbox(mboxName)
			if err == nil {
				d.mboxes = append(d.mboxes, *mbox.(*Mailbox))
				continue
			}
		}

		mbox, err := u.GetMailbox(name)
		if err != nil {
			if err != backend.ErrNoSuchMailbox {
				d.mboxes = nil
				return err
			}

			if err := u.CreateMailbox(name); err != nil && err != backend.ErrMailboxAlreadyExists {
				d.mboxes = nil
				return err
			}

			mbox, err = u.GetMailbox(name)
			if err != nil {
				d.mboxes = nil
				return err
			}
		}

		d.mboxes = append(d.mboxes, *mbox.(*Mailbox))
	}
	return nil
}

// SpecialMailbox is similar to Mailbox method but instead of looking up mailboxes
// by name it looks it up by the SPECIAL-USE attribute.
//
// If no such mailbox exists for some user, it will be created with
// fallbackName and requested SPECIAL-USE attribute set.
//
// The main use-case of this function is to reroute messages into Junk directory
// during multi-recipient delivery.
func (d *Delivery) SpecialMailbox(attribute, fallbackName string) error {
	if cap(d.mboxes) < len(d.users) {
		d.mboxes = make([]Mailbox, 0, len(d.users))
	}
	for _, u := range d.users {
		if mboxName := d.mboxOverrides[u.username]; mboxName != "" {
			mbox, err := u.GetMailbox(mboxName)
			if err == nil {
				d.mboxes = append(d.mboxes, *mbox.(*Mailbox))
				continue
			}
		}

		var mboxId uint64
		var mboxName string
		err := d.b.specialUseMbox.QueryRow(u.id, attribute).Scan(&mboxName, &mboxId)
		if err != nil {
			if err != sql.ErrNoRows {
				d.mboxes = nil
				return err
			}

			if err := u.CreateMailboxSpecial(fallbackName, attribute); err != nil && err != backend.ErrMailboxAlreadyExists {
				d.mboxes = nil
				return err
			}

			mbox, err := u.GetMailbox(fallbackName)
			if err != nil {
				d.mboxes = nil
				return err
			}
			d.mboxes = append(d.mboxes, *mbox.(*Mailbox))
			continue
		}

		d.mboxes = append(d.mboxes, Mailbox{user: u, id: mboxId, name: mboxName, parent: d.b})
	}
	return nil
}

func (d *Delivery) UserMailbox(username, mailbox string, flags []string) {
	if d.mboxOverrides == nil {
		d.mboxOverrides = make(map[string]string)
	}
	if d.flagOverrides == nil {
		d.flagOverrides = make(map[string][]string)
	}

	d.mboxOverrides[username] = mailbox
	d.flagOverrides[username] = flags
}

type memoryBuffer struct {
	slice []byte
}

func (mb memoryBuffer) Open() (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewReader(mb.slice)), nil
}

// BodyRaw is convenience wrapper for BodyParsed. Use it only for most simple cases (e.g. for tests).
//
// You want to use BodyParsed in most cases. It is much more efficient. BodyRaw reads the entire message
// into memory.
func (d *Delivery) BodyRaw(message io.Reader) error {
	bufferedMsg := bufio.NewReader(message)
	hdr, err := textproto.ReadHeader(bufferedMsg)
	if err != nil {
		return err
	}

	blob, err := ioutil.ReadAll(bufferedMsg)
	if err != nil {
		return err
	}

	return d.BodyParsed(hdr, len(blob), memoryBuffer{slice: blob})
}

// Buffer is the temporary storage for the message body.
type Buffer interface {
	Open() (io.ReadCloser, error)
}

func (d *Delivery) BodyParsed(header textproto.Header, bodyLen int, body Buffer) error {
	if len(d.mboxes) == 0 {
		if err := d.Mailbox("INBOX"); err != nil {
			return err
		}
	}

	if cap(d.updates) < len(d.mboxes) {
		d.updates = make([]backend.Update, 0, len(d.mboxes))
	}

	// Make sure all auto-generated statements are generated before we start transaction
	// so it will not cause deadlocks on SQlite when statement is prepared outside
	// of transaction while transaction is running.
	for _, mbox := range d.mboxes {
		_, err := d.b.getFlagsAddStmt(true, append([]string{imap.RecentFlag}, d.flagOverrides[mbox.user.username]...))
		if err != nil {
			return wrapErr(err, "Body")
		}
	}

	date := time.Now()

	var err error
	d.tx, err = d.b.db.BeginLevel(sql.LevelReadCommitted, false)
	if err != nil {
		return wrapErr(err, "Body")
	}

	for _, mbox := range d.mboxes {
		flagsStmt, err := d.b.getFlagsAddStmt(true, append([]string{imap.RecentFlag}, d.flagOverrides[mbox.user.username]...))
		if err != nil {
			return wrapErr(err, "Body")
		}

		err = d.mboxDelivery(header, mbox, int64(bodyLen), body, date, flagsStmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Delivery) mboxDelivery(header textproto.Header, mbox Mailbox, bodyLen int64, body Buffer, date time.Time, flagsStmt *sql.Stmt) (err error) {
	header = header.Copy()
	userHeader := d.perRcptHeader[mbox.user.username]
	for fields := userHeader.Fields(); fields.Next(); {
		header.Add(fields.Key(), fields.Value())
	}

	headerBlob := bytes.Buffer{}
	if err := textproto.WriteHeader(&headerBlob, header); err != nil {
		return wrapErr(err, "Body (WriteHeader)")
	}

	length := int64(headerBlob.Len()) + bodyLen
	bodyReader, err := body.Open()
	if err != nil {
		return err
	}

	bodyStruct, cachedHeader, extBodyKey, err := d.b.processParsedBody(headerBlob.Bytes(), header, bodyReader, bodyLen)
	if err != nil {
		return err
	}

	if _, err = d.tx.Stmt(d.b.addExtKey).Exec(extBodyKey, mbox.user.id, 1); err != nil {
		d.b.extStore.Delete([]string{extBodyKey})
		return wrapErr(err, "Body (addExtKey)")
	}

	// Note that we are extremely careful here with ordering to
	// decrease change of deadlocks as a result of transaction
	// serialization.

	// --- operations that involve mboxes table ---
	msgId, err := mbox.incrementMsgCounters(d.tx)
	if err != nil {
		d.b.extStore.Delete([]string{extBodyKey})
		return wrapErr(err, "Body (incrementMsgCounters)")
	}

	upd, err := mbox.statusUpdate(d.tx)
	if err != nil {
		d.b.extStore.Delete([]string{extBodyKey})
		return wrapErr(err, "Body (statusUpdate)")
	}
	d.updates = append(d.updates, upd)
	// --- end of operations that involve mboxes table ---

	// --- operations that involve msgs table ---
	_, err = d.tx.Stmt(d.b.addMsg).Exec(
		mbox.id, msgId, date.Unix(),
		length,
		bodyStruct, cachedHeader, extBodyKey,
		0, d.b.Opts.CompressAlgo,
	)
	if err != nil {
		d.b.extStore.Delete([]string{extBodyKey})
		return wrapErr(err, "Body (addMsg)")
	}
	// --- end of operations that involve msgs table ---

	// --- operations that involve flags table ---
	flags := []string{imap.RecentFlag}
	flags = append(flags, d.flagOverrides[mbox.user.username]...)

	params := mbox.makeFlagsAddStmtArgs(true, flags, msgId, msgId)
	if _, err := d.tx.Stmt(flagsStmt).Exec(params...); err != nil {
		d.b.extStore.Delete([]string{extBodyKey})
		return wrapErr(err, "Body (flagsStmt)")
	}
	// --- end operations that involve flags table ---

	return nil
}

func (d *Delivery) Abort() error {
	if d.tx != nil {
		if err := d.tx.Rollback(); err != nil {
			return err
		}
	}
	if d.extKey != "" {
		if err := d.b.extStore.Delete([]string{d.extKey}); err != nil {
			return err
		}
	}

	d.clean()
	return nil
}

// Commit finishes the delivery.
//
// If this function returns no error - the message is successfully added to the mailbox
// of *all* recipients.
//
// After Commit or Abort is called, Delivery object can be reused as if it was
// just created.
func (d *Delivery) Commit() error {
	if d.tx != nil {
		if err := d.tx.Commit(); err != nil {
			return err
		}
	}
	if d.b.updates != nil {
		for _, update := range d.updates {
			d.b.updates <- update
		}
	}

	d.clean()
	return nil
}

func (b *Backend) processParsedBody(headerInput []byte, header textproto.Header, bodyLiteral io.Reader, bodyLen int64) (bodyStruct, cachedHeader []byte, extBodyKey string, err error) {
	extBodyKey, err = randomKey()
	if err != nil {
		return nil, nil, "", err
	}

	objSize := bodyLen
	if b.Opts.CompressAlgo != "" {
		objSize = -1
	}

	extWriter, err := b.extStore.Create(extBodyKey, objSize)
	if err != nil {
		return nil, nil, "", err
	}
	defer extWriter.Close()

	compressW, err := b.compressAlgo.WrapCompress(extWriter, b.Opts.CompressAlgoParams)
	if err != nil {
		return nil, nil, "", err
	}
	defer compressW.Close()

	if _, err := compressW.Write(headerInput); err != nil {
		b.extStore.Delete([]string{extBodyKey})
		return nil, nil, "", err
	}

	bufferedBody := bufio.NewReader(io.TeeReader(bodyLiteral, compressW))
	bodyStruct, cachedHeader, err = extractCachedData(header, bufferedBody)
	if err != nil {
		b.extStore.Delete([]string{extBodyKey})
		return nil, nil, "", err
	}

	// Consume all remaining body so io.TeeReader used with external store will
	// copy everything to extWriter.
	_, err = io.Copy(ioutil.Discard, bufferedBody)
	if err != nil {
		b.extStore.Delete([]string{extBodyKey})
		return nil, nil, "", err
	}

	if err := extWriter.Sync(); err != nil {
		return nil, nil, "", err
	}

	return
}
