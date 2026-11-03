package imapsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
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
// Message bodies are staged until Commit. The recipient or mailbox can be
// deleted or changed between AddRcpt, Body and Commit; in that case Commit
// returns ErrDeliveryInterrupted.
// Sender should retry delivery after a short delay.
func (b *Backend) NewDelivery() Delivery {
	return b.NewDeliveryWithOptions(DeliveryOptions{})
}

func (b *Backend) NewDeliveryWithOptions(opts DeliveryOptions) Delivery {
	return Delivery{b: b, opts: opts, perRcptHeader: map[string]textproto.Header{}}
}

func (d *Delivery) clean() {
	d.users = d.users[0:0]
	d.mboxes = d.mboxes[0:0]
	d.staged = nil
	d.flagOverrides = nil
	d.mboxOverrides = nil
	d.pendingSpecial = nil
	d.specialChecks = nil
	d.createdMboxes = nil
	d.bodyStarted = false
	d.bodyErr = nil
	d.commitFailed = false
	for k := range d.perRcptHeader {
		delete(d.perRcptHeader, k)
	}
}

type DeliveryOptions struct {
	SkipNotify bool
}

type Delivery struct {
	b    *Backend
	opts DeliveryOptions

	users  []User
	mboxes []Mailbox

	staged         []stagedMessage
	perRcptHeader  map[string]textproto.Header
	flagOverrides  map[string][]string
	mboxOverrides  map[string]string
	pendingSpecial map[uint64]string
	specialChecks  map[uint64]deliverySpecialCheck
	createdMboxes  map[deliveryMailboxKey]uint64
	bodyStarted    bool
	bodyErr        error
	commitFailed   bool
}

type deliveryMailboxKey struct {
	user uint64
	name string
}

type deliverySpecialCheck struct {
	attribute string
	mailboxID uint64
}

type stagedMessage struct {
	mbox         Mailbox
	length       int64
	bodyStruct   []byte
	cachedHeader []byte
	extKey       string
	flags        []string
	date         time.Time
	msgID        uint32
}

// AddRcpt adds the recipient username/mailbox pair to the delivery.
//
// If this function returns an error - further calls will still work
// correctly and there is no need to restart the delivery.
//
// The specified user account should exist at the time AddRcpt is called. If
// the account or its selected mailbox disappears before Commit, Commit returns
// ErrDeliveryInterrupted.
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
// If it is not called, it defaults to INBOX. If the mailbox doesn't exist for
// some users, it will be created during Commit.
func (d *Delivery) Mailbox(name string) error {
	d.mboxes = make([]Mailbox, 0, len(d.users))
	d.pendingSpecial = nil
	d.specialChecks = nil

	for _, u := range d.users {
		if mboxName := d.mboxOverrides[u.username]; mboxName != "" {
			_, mbox, err := u.GetMailbox(mboxName, true, nil)
			if err == nil {
				d.mboxes = append(d.mboxes, *mbox.(*Mailbox))
				continue
			}
		}

		_, mbox, err := u.GetMailbox(name, true, nil)
		if err != nil {
			if err != backend.ErrNoSuchMailbox {
				d.mboxes = nil
				return err
			}

			d.mboxes = append(d.mboxes, Mailbox{user: u, name: name, parent: d.b})
			continue
		}

		d.mboxes = append(d.mboxes, *mbox.(*Mailbox))
	}
	return nil
}

// SpecialMailbox is similar to Mailbox method but instead of looking up mailboxes
// by name it looks it up by the SPECIAL-USE attribute.
//
// If no such mailbox exists for some user, it will be created during Commit
// with fallbackName and the requested SPECIAL-USE attribute set.
//
// The main use-case of this function is to reroute messages into Junk directory
// during multi-recipient delivery.
func (d *Delivery) SpecialMailbox(attribute, fallbackName string) error {
	switch attribute {
	case imap.ArchiveAttr, imap.DraftsAttr, imap.JunkAttr, imap.SentAttr, imap.TrashAttr:
	default:
		return ErrUnsupportedSpecialAttr
	}
	d.mboxes = make([]Mailbox, 0, len(d.users))
	d.pendingSpecial = make(map[uint64]string)
	d.specialChecks = make(map[uint64]deliverySpecialCheck)
	for _, u := range d.users {
		if mboxName := d.mboxOverrides[u.username]; mboxName != "" {
			_, mbox, err := u.GetMailbox(mboxName, true, nil)
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
			d.specialChecks[u.id] = deliverySpecialCheck{attribute: attribute}

			_, mbox, err := u.GetMailbox(fallbackName, true, nil)
			if err != nil {
				if err != backend.ErrNoSuchMailbox {
					d.mboxes = nil
					return err
				}
				d.mboxes = append(d.mboxes, Mailbox{user: u, name: fallbackName, parent: d.b})
				d.pendingSpecial[u.id] = attribute
				continue
			}
			d.mboxes = append(d.mboxes, *mbox.(*Mailbox))
			continue
		}

		d.specialChecks[u.id] = deliverySpecialCheck{attribute: attribute, mailboxID: mboxId}
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
func (d *Delivery) BodyRaw(message io.Reader) (err error) {
	defer func() {
		if err != nil {
			d.bodyErr = err
		}
	}()
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

func (d *Delivery) BodyParsed(header textproto.Header, bodyLen int, body Buffer) (err error) {
	if d.bodyStarted || d.bodyErr != nil {
		err := errors.New("delivery: body already provided or failed")
		d.bodyErr = err
		return err
	}
	d.bodyStarted = true
	defer func() {
		if err != nil {
			d.bodyErr = err
		}
	}()
	if len(d.mboxes) == 0 {
		if err := d.Mailbox("INBOX"); err != nil {
			return err
		}
	}

	// Make sure all auto-generated statements are generated before we start transaction
	// so it will not cause deadlocks on SQlite when statement is prepared outside
	// of transaction while transaction is running.
	for _, mbox := range d.mboxes {
		if len(d.flagOverrides[mbox.user.username]) != 0 {
			_, err := d.b.getFlagsAddStmt(len(d.flagOverrides[mbox.user.username]))
			if err != nil {
				return wrapErr(err, "Body")
			}
		}
	}

	for _, mbox := range d.mboxes {
		if err := d.stageMailbox(header, mbox, int64(bodyLen), body); err != nil {
			return err
		}
	}

	return nil
}

func (d *Delivery) stageMailbox(header textproto.Header, mbox Mailbox, bodyLen int64, body Buffer) error {
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
	defer bodyReader.Close()

	bodyStruct, cachedHeader, extBodyKey, err := d.b.processParsedBody(headerBlob.Bytes(), header, bodyReader, bodyLen)
	if err != nil {
		if extBodyKey != "" {
			d.staged = append(d.staged, stagedMessage{extKey: extBodyKey})
		}
		return err
	}

	d.staged = append(d.staged, stagedMessage{
		mbox:         mbox,
		length:       length,
		bodyStruct:   bodyStruct,
		cachedHeader: cachedHeader,
		extKey:       extBodyKey,
		flags:        append([]string(nil), d.flagOverrides[mbox.user.username]...),
		date:         time.Now(),
	})
	return nil
}

func (d *Delivery) commitMailbox(tx *sql.Tx, msg *stagedMessage) error {
	mbox := &msg.mbox

	// Note that we are extremely careful here with ordering to
	// decrease change of deadlocks as a result of transaction
	// serialization.

	// --- operations that involve mboxes table ---

	// Referred special mailbox lookup, if mailbox moved or renamed - interrupt delivery.
	var id uint64
	if check, ok := d.specialChecks[mbox.user.id]; ok {
		var currentID uint64
		err := tx.Stmt(d.b.specialUseMbox).QueryRow(mbox.user.id, check.attribute).Scan(new(string), &currentID)
		if err != nil && err != sql.ErrNoRows {
			return wrapErrf(err, "Commit (validate special mailbox)")
		}
		if currentID != check.mailboxID {
			return ErrDeliveryInterrupted
		}
	}

	if mbox.id == 0 {
		key := deliveryMailboxKey{user: mbox.user.id, name: mbox.name}
		if id := d.createdMboxes[key]; id != 0 {
			mbox.id = id
		} else {
			err := tx.QueryRow(d.b.db.rewriteSQL(`SELECT id FROM users WHERE id = ? AND username = ?`), mbox.user.id, mbox.user.username).Scan(&id)
			if err == sql.ErrNoRows {
				return ErrDeliveryInterrupted
			}
			if err != nil {
				return wrapErr(err, "Commit (validate user)")
			}
			if err := mbox.user.createParentDirs(tx, mbox.name); err != nil {
				return wrapErr(err, "Commit (create parent mailboxes)")
			}
			var special interface{}
			if attr := d.pendingSpecial[mbox.user.id]; attr != "" {
				special = attr
			}
			if _, err := tx.Stmt(d.b.createMbox).Exec(mbox.user.id, mbox.name, d.b.prng.Uint32(), special); err != nil {
				if isForeignKeyErr(err) {
					return ErrDeliveryInterrupted
				}
				return wrapErrf(err, "Commit (create mailbox)")
			}
			if err := tx.Stmt(d.b.mboxId).QueryRow(mbox.user.id, mbox.name).Scan(&mbox.id); err != nil {
				return wrapErr(err, "Commit (get mailbox)")
			}
			if d.createdMboxes == nil {
				d.createdMboxes = make(map[deliveryMailboxKey]uint64)
			}
			d.createdMboxes[key] = mbox.id
			if check, ok := d.specialChecks[mbox.user.id]; ok && d.pendingSpecial[mbox.user.id] != "" {
				check.mailboxID = mbox.id
				d.specialChecks[mbox.user.id] = check
			}
		}
	}

	// Check that the target mailbox haven't been moved, renamed or deleted.
	name := mbox.name
	if strings.EqualFold(name, imap.InboxName) {
		name = imap.InboxName
	}
	err := tx.QueryRow(
		d.b.db.rewriteSQL(`SELECT id FROM mboxes WHERE id = ? AND uid = ? AND name = ?`),
		mbox.id, mbox.user.id, name,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return ErrDeliveryInterrupted
	}
	if err != nil {
		return wrapErrf(err, "Commit (validate mailbox)")
	}
	if _, err = tx.Stmt(d.b.addExtKey).Exec(msg.extKey, mbox.user.id, 1); err != nil {
		return wrapErrf(err, "Commit (addExtKey)")
	}

	msgId, err := mbox.incrementMsgCounters(tx)
	if err != nil {
		return wrapErrf(err, "Commit (incrementMsgCounters)")
	}
	msg.msgID = msgId

	// --- end of operations that involve mboxes table ---

	// --- operations that involve msgs table ---
	haveSeen := 0
	for _, flag := range msg.flags {
		if flag == imap.SeenFlag {
			haveSeen = 1
			break
		}
	}

	_, err = tx.Stmt(d.b.addMsg).Exec(
		mbox.id, msgId, msg.date.Unix(),
		msg.length,
		msg.bodyStruct, msg.cachedHeader, msg.extKey,
		haveSeen, d.b.Opts.CompressAlgo, 1,
	)
	if err != nil {
		return wrapErrf(err, "Commit (addMsg)")
	}
	// --- end of operations that involve msgs table ---

	// --- operations that involve flags table ---
	flags := msg.flags
	if len(flags) != 0 {
		flagsStmt, err := d.b.getFlagsAddStmt(len(flags))
		if err != nil {
			return wrapErrf(err, "Commit (flagsStmt)")
		}
		params := mbox.makeFlagsAddStmtArgs(flags, msgId, msgId)
		if _, err := tx.Stmt(flagsStmt).Exec(params...); err != nil {
			return wrapErr(err, "Commit (flagsStmt)")
		}
	}
	// --- end operations that involve flags table ---

	return nil
}

func (d *Delivery) Abort() error {
	if len(d.staged) != 0 {
		keys := make([]string, 0, len(d.staged))
		for _, msg := range d.staged {
			if d.commitFailed {
				// COMMIT for PostgreSQL can fail on network error. Don't remove extKeys
				// that are actually referenced in the DB.
				var count int
				if err := d.b.db.QueryRow(`SELECT COUNT(*) FROM extKeys WHERE id = ?`, msg.extKey).Scan(&count); err != nil {
					d.b.Opts.Log.Println("check commited extKeys error:", err)
					continue
				}
				if count != 0 {
					continue
				}
			}
			keys = append(keys, msg.extKey)
		}
		if err := d.b.extStore.Delete(keys); err != nil {
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
	if d.bodyErr != nil {
		err := d.bodyErr
		if abortErr := d.Abort(); abortErr != nil {
			return fmt.Errorf("%v; abort: %w", err, abortErr)
		}
		return err
	}
	if len(d.staged) == 0 {
		d.clean()
		return nil
	}

	tx, err := d.b.db.BeginLevel(sql.LevelReadCommitted, false)
	if err != nil {
		if abortErr := d.Abort(); abortErr != nil {
			return fmt.Errorf("%v; abort: %w", err, abortErr)
		}
		return wrapErrf(err, "Commit (begin)")
	}

	defer func(tx *sql.Tx) {
		err := tx.Rollback()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			d.b.Opts.Log.Println("delivery.Commit tx.Rollback:", err)
		}
	}(tx)

	for i := range d.staged {
		if err := d.commitMailbox(tx, &d.staged[i]); err != nil {
			if abortErr := d.Abort(); abortErr != nil {
				return fmt.Errorf("%v; abort: %w", err, abortErr)
			}
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		d.commitFailed = true
		if abortErr := d.Abort(); abortErr != nil {
			return fmt.Errorf("%v; abort: %w", err, abortErr)
		}
		return wrapErrf(err, "Commit")
	}

	for _, msg := range d.staged {
		if !d.b.mngr.NewMessage(msg.mbox.id, msg.msgID) {
			if _, err := d.b.db.Exec(`UPDATE msgs SET recent = 0 WHERE mboxId = ? AND msgId = ?`, msg.mbox.id, msg.msgID); err != nil {
				d.b.Opts.Log.Printf("delivery: failed to clear recent flag: %v", err)
			}
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
	key := extBodyKey
	defer func() {
		if err != nil {
			if cleanupErr := b.extStore.Delete([]string{key}); cleanupErr != nil {
				extBodyKey = key
				err = fmt.Errorf("%v; delete staged body: %w", err, cleanupErr)
			}
		}
	}()

	objSize := int64(len(headerInput)) + bodyLen
	if b.Opts.CompressAlgo != "" {
		objSize = -1
	}

	extWriter, err := b.extStore.Create(extBodyKey, objSize)
	if err != nil {
		return nil, nil, "", err
	}
	defer func() {
		if extWriter != nil {
			if closeErr := extWriter.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
	}()

	compressW, err := b.compressAlgo.WrapCompress(extWriter, b.Opts.CompressAlgoParams)
	if err != nil {
		return nil, nil, "", err
	}
	defer func() {
		if compressW != nil {
			if closeErr := compressW.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
	}()

	if _, err := compressW.Write(headerInput); err != nil {
		return nil, nil, "", err
	}

	bufferedBody := bufio.NewReader(io.TeeReader(bodyLiteral, compressW))
	bodyStruct, cachedHeader, _, err = extractCachedData(header, bufferedBody)
	if err != nil {
		return nil, nil, "", err
	}

	// Consume all remaining body so io.TeeReader used with external store will
	// copy everything to extWriter.
	_, err = io.Copy(ioutil.Discard, bufferedBody)
	if err != nil {
		return nil, nil, "", err
	}

	if err := compressW.Close(); err != nil {
		compressW = nil
		return nil, nil, "", err
	}
	compressW = nil
	if err := extWriter.Sync(); err != nil {
		return nil, nil, "", err
	}
	if err := extWriter.Close(); err != nil {
		extWriter = nil
		return nil, nil, "", err
	}
	extWriter = nil

	return
}
