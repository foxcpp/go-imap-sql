package imapsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"io/ioutil"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-message/textproto"
	"github.com/pkg/errors"
)

var ErrDeliveryInterrupted = errors.New("sql: delivery transaction interrupted, try again later")

// StartDelivery starts an atomic message delivery session.
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
func (b *Backend) StartDelivery() (*Delivery, error) {
	return &Delivery{b: b}, nil
}

type Delivery struct {
	b       *Backend
	tx      *sql.Tx
	users   []*User
	mboxes  []*Mailbox
	extKey  string
	updates []backend.Update
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
func (d *Delivery) AddRcpt(username string) error {
	u, err := d.b.GetUser(username)
	if err != nil {
		return err
	}
	d.users = append(d.users, u.(*User))
	return nil
}

// FIXME: Fix that goddamned code duplication.

// Mailbox command changes the target mailbox for all recipients.
// It should be called before BodyParsed/BodyRaw.
//
// If it is not called, it defaults to INBOX. If mailbox doesn't
// exist for some users - it will created.
func (d *Delivery) Mailbox(name string) error {
	d.mboxes = make([]*Mailbox, 0, len(d.users))
	for _, u := range d.users {
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

		d.mboxes = append(d.mboxes, mbox.(*Mailbox))
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
	d.mboxes = make([]*Mailbox, 0, len(d.users))
	for _, u := range d.users {
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
			d.mboxes = append(d.mboxes, mbox.(*Mailbox))
			continue
		}

		d.mboxes = append(d.mboxes, &Mailbox{user: u, uid: u.id, id: mboxId, name: mboxName, parent: d.b})
	}
	return nil
}

// BodyRaw assigns the raw message blob to the delivery.
//
// go-imap-sql needs to parse the header to extract the envelope information
// so if you already have parsed header object - it is preferable to use BodyParsed
// method instead of BodyRaw.
//
// Also note that BodyRaw/BodyParsed can be called only once for a Delivery object.
// Behavior is undefined when it is called multiple times.
func (d *Delivery) BodyRaw(fullMsg imap.Literal) error {
	if d.mboxes == nil {
		if err := d.Mailbox("INBOX"); err != nil {
			return err
		}
	}

	d.updates = make([]backend.Update, 0, len(d.mboxes))
	flagsStmt, err := d.b.makeFlagsAddStmt(true, []string{imap.RecentFlag})
	if err != nil {
		return errors.Wrap(err, "Body")
	}

	date := time.Now()

	d.tx, err = d.b.db.Begin()
	if err != nil {
		return errors.Wrap(err, "Body")
	}

	headerBlob, bodyBlob, bodyStruct, cachedHeader, extBodyKey, err := d.b.processBody(fullMsg)
	if err != nil {
		return err
	}

	if extBodyKey.Valid {
		if _, err = d.tx.Stmt(d.b.addExtKey).Exec(extBodyKey, len(d.mboxes)); err != nil {
			return errors.Wrap(err, "Body")
		}
	}

	for _, mbox := range d.mboxes {
		msgId, err := mbox.uidNext(d.tx)
		if err != nil {
			return errors.Wrap(err, "Body")
		}

		_, err = d.tx.Stmt(d.b.addMsg).Exec(
			mbox.id, msgId, date.Unix(),
			fullMsg.Len(), bodyBlob, headerBlob,
			bodyStruct, cachedHeader, extBodyKey,
		)
		if err != nil {
			return errors.Wrap(err, "Body")
		}

		params := mbox.makeFlagsAddStmtArgs(true, []string{imap.RecentFlag}, imap.Seq{Start: msgId, Stop: msgId})
		if _, err := d.tx.Stmt(flagsStmt).Exec(params...); err != nil {
			return errors.Wrap(err, "Body")
		}

		if _, err := d.tx.Stmt(d.b.addUidNext).Exec(1, mbox.id); err != nil {
			return errors.Wrap(err, "Body")
		}

		upd, err := mbox.statusUpdate(d.tx)
		if err != nil {
			return errors.Wrap(err, "Body")
		}
		d.updates = append(d.updates, upd)
	}

	return nil
}

func (d *Delivery) BodyParsed(header textproto.Header, body imap.Literal) error {
	if d.mboxes == nil {
		if err := d.Mailbox("INBOX"); err != nil {
			return err
		}
	}

	d.updates = make([]backend.Update, 0, len(d.mboxes))
	flagsStmt, err := d.b.makeFlagsAddStmt(true, []string{imap.RecentFlag})
	if err != nil {
		return errors.Wrap(err, "Body")
	}

	date := time.Now()

	d.tx, err = d.b.db.Begin()
	if err != nil {
		return errors.Wrap(err, "Body")
	}

	headerBlob := bytes.Buffer{}
	if err := textproto.WriteHeader(&headerBlob, header); err != nil {
		return errors.Wrap(err, "Body")
	}

	bodyLen := headerBlob.Len() + body.Len()

	headerBlobField, bodyBlob, bodyStruct, cachedHeader, extBodyKey, err := d.b.processParsedBody(headerBlob.Bytes(), header, body)
	if err != nil {
		return err
	}

	if extBodyKey.Valid {
		if _, err = d.tx.Stmt(d.b.addExtKey).Exec(extBodyKey, len(d.mboxes)); err != nil {
			return errors.Wrap(err, "Body")
		}
	}

	for _, mbox := range d.mboxes {
		msgId, err := mbox.uidNext(d.tx)
		if err != nil {
			return errors.Wrap(err, "Body")
		}

		_, err = d.tx.Stmt(d.b.addMsg).Exec(
			mbox.id, msgId, date.Unix(),
			bodyLen, bodyBlob, headerBlobField,
			bodyStruct, cachedHeader, extBodyKey,
		)
		if err != nil {

			return errors.Wrap(err, "Body")
		}

		params := mbox.makeFlagsAddStmtArgs(true, []string{imap.RecentFlag}, imap.Seq{Start: msgId, Stop: msgId})
		if _, err := d.tx.Stmt(flagsStmt).Exec(params...); err != nil {
			return errors.Wrap(err, "Body")
		}

		if _, err := d.tx.Stmt(d.b.addUidNext).Exec(1, mbox.id); err != nil {
			return errors.Wrap(err, "Body")
		}

		upd, err := mbox.statusUpdate(d.tx)
		if err != nil {
			return errors.Wrap(err, "Body")
		}
		d.updates = append(d.updates, upd)
	}

	return nil
}

func (d *Delivery) Abort() error {
	if d.tx != nil {
		if err := d.tx.Rollback(); err != nil {
			return err
		}
	}
	if d.extKey != "" {
		if err := d.b.Opts.ExternalStore.Delete([]string{d.extKey}); err != nil {
			return err
		}
	}
	return nil
}

// Commit finishes the delivery.
//
// If this function returns no error - the message is successfully added to the mailbox
// of *all* recipients.
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
	return nil
}

func (b *Backend) processParsedBody(headerInput []byte, header textproto.Header, bodyLiteral imap.Literal) (headerBlob, bodyBlob, bodyStruct, cachedHeader []byte, extBodyKey sql.NullString, err error) {
	var bodyReader io.Reader = bodyLiteral
	if b.Opts.ExternalStore != nil {
		extBodyKey.String, err = randomKey()
		if err != nil {
			return nil, nil, nil, nil, sql.NullString{}, err
		}
		extBodyKey.Valid = true
		extWriter, err := b.Opts.ExternalStore.Create(extBodyKey.String)
		if err != nil {
			return nil, nil, nil, nil, sql.NullString{}, err
		}
		defer extWriter.Close()

		if _, err := extWriter.Write(headerInput); err != nil {
			return nil, nil, nil, nil, sql.NullString{}, err
		}

		bodyReader = io.TeeReader(bodyLiteral, extWriter)

		headerBlob = nil
		bodyBlob = nil
	} else {
		bodyBuf, err := ioutil.ReadAll(bodyLiteral)
		if err != nil {
			return nil, nil, nil, nil, sql.NullString{}, err
		}
		bodyReader = bytes.NewReader(bodyBuf)

		// TODO: Make an approximation for buffer size so we can pre-allocate it.
		headerBuf := bytes.Buffer{}
		if err := textproto.WriteHeader(&headerBuf, header); err != nil {
			return nil, nil, nil, nil, sql.NullString{}, err
		}
	}

	bufferedBody := bufio.NewReader(bodyReader)
	bodyStruct, cachedHeader, err = extractCachedData(header, bufferedBody)
	if err != nil {
		return nil, nil, nil, nil, sql.NullString{}, err
	}

	// Consume all remaining body so io.TeeReader used with external store will
	// copy everything to extWriter.
	_, err = io.Copy(ioutil.Discard, bufferedBody)
	if err != nil {
		return nil, nil, nil, nil, sql.NullString{}, err
	}

	return
}
