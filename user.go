package imapsql

import (
	"database/sql"
	"strings"

	"github.com/emersion/go-imap-specialuse"
	"github.com/emersion/go-imap/backend"
	"github.com/pkg/errors"
)

const MailboxPathSep = "."

type User struct {
	id       uint64
	username string
	inboxId  uint64
	parent   *Backend
}

func (u *User) Username() string {
	return u.username
}

func (u *User) ID() uint64 {
	return u.id
}

func (u *User) ListMailboxes(subscribed bool) ([]backend.Mailbox, error) {
	var rows *sql.Rows
	var err error
	if subscribed {
		rows, err = u.parent.listSubbedMboxes.Query(u.id)
	} else {
		rows, err = u.parent.listMboxes.Query(u.id)
	}
	if err != nil {
		return nil, errors.Wrap(err, "ListMailboxes")
	}
	defer rows.Close()

	res := []backend.Mailbox{}
	for rows.Next() {
		id, name := uint64(0), ""
		if err := rows.Scan(&id, &name); err != nil {
			return nil, errors.Wrap(err, "ListMailboxes")
		}

		res = append(res, &Mailbox{user: u, uid: u.id, id: id, name: name, parent: u.parent})
	}
	return res, errors.Wrap(rows.Err(), "ListMailboxes")
}

func (u *User) GetMailbox(name string) (backend.Mailbox, error) {
	if strings.EqualFold(name, "INBOX") {
		return &Mailbox{user: u, uid: u.id, id: u.inboxId, name: name, parent: u.parent}, nil
	}

	row := u.parent.mboxId.QueryRow(u.id, name)
	id := uint64(0)
	if err := row.Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return nil, backend.ErrNoSuchMailbox
		}
		return nil, errors.Wrapf(err, "GetMailbox %s", name)
	}

	return &Mailbox{user: u, uid: u.id, id: id, name: name, parent: u.parent}, nil
}

func (u *User) CreateMessageLimit() *uint32 {
	res := sql.NullInt64{}
	row := u.parent.userMsgSizeLimit.QueryRow(u.id)
	if err := row.Scan(&res); err != nil {
		// Oops!
		return new(uint32)
	}

	if !res.Valid {
		return nil
	} else {
		val := uint32(res.Int64)
		return &val
	}
}

func (u *User) SetMessageLimit(val *uint32) error {
	_, err := u.parent.setUserMsgSizeLimit.Exec(val, u.id)
	return err
}

func (u *User) CreateMailbox(name string) error {
	tx, err := u.parent.db.Begin(false)
	if err != nil {
		return errors.Wrapf(err, "CreateMailbox %s", name)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := u.createParentDirs(tx, name); err != nil {
		return errors.Wrapf(err, "CreateMailbox (parents) %s", name)
	}

	if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, name, u.parent.prng.Uint32(), nil); err != nil {
		if isForeignKeyErr(err) {
			return backend.ErrMailboxAlreadyExists
		}
		return errors.Wrapf(err, "CreateMailbox %s", name)
	}

	return errors.Wrapf(tx.Commit(), "CreateMailbox %s", name)
}

var ErrUnsupportedSpecialAttr = errors.New("imap: special attribute is not supported")

// CreateMailboxSpecial creates a mailbox with SPECIAL-USE attribute set.
func (u *User) CreateMailboxSpecial(name, specialUseAttr string) error {
	switch specialUseAttr {
	case specialuse.All, specialuse.Flagged:
		return ErrUnsupportedSpecialAttr
	case specialuse.Archive, specialuse.Drafts, specialuse.Junk, specialuse.Sent, specialuse.Trash:
	default:
		return ErrUnsupportedSpecialAttr
	}

	tx, err := u.parent.db.Begin(false)
	if err != nil {
		return errors.Wrapf(err, "CreateMailboxSpecial %s", name)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := u.createParentDirs(tx, name); err != nil {
		return errors.Wrapf(err, "CreateMailboxSpecial (parents) %s", name)
	}

	if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, name, u.parent.prng.Uint32(), specialUseAttr); err != nil {
		if isForeignKeyErr(err) {
			return backend.ErrMailboxAlreadyExists
		}
		return errors.Wrapf(err, "CreateMailboxSpecial %s", name)
	}

	return errors.Wrapf(tx.Commit(), "CreateMailbox %s", name)
}

func (u *User) DeleteMailbox(name string) error {
	if strings.ToLower(name) == "inbox" {
		return errors.New("DeleteMailbox: can't delete INBOX")
	}

	tx, err := u.parent.db.BeginLevel(sql.LevelRepeatableRead, false)
	if err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	}
	defer tx.Rollback()

	if _, err := tx.Stmt(u.parent.decreaseRefForMbox).Exec(u.id, name); err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	}

	rows, err := tx.Stmt(u.parent.zeroRefUser).Query(u.id)
	if err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	}
	defer rows.Close()

	keys := make([]string, 0, 16)
	for rows.Next() {
		var extKey string
		if err := rows.Scan(&extKey); err != nil {
			return errors.Wrapf(err, "DeleteMailbox %s", name)
		}
		keys = append(keys, extKey)

	}

	if err := u.parent.Opts.ExternalStore.Delete(keys); err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	}

	// TODO: Grab mboxId along the way on PostgreSQL?
	stats, err := tx.Stmt(u.parent.deleteMbox).Exec(u.id, name)
	if err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	}
	if affected == 0 {
		return backend.ErrNoSuchMailbox
	}

	if _, err := tx.Stmt(u.parent.deleteZeroRef).Exec(u.id); err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	}

	return tx.Commit()
}

func (u *User) RenameMailbox(existingName, newName string) error {
	tx, err := u.parent.db.Begin(false)
	if err != nil {
		return errors.Wrapf(err, "RenameMailbox %s, %s", existingName, newName)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := u.createParentDirs(tx, newName); err != nil {
		return errors.Wrapf(err, "RenameMailbox %s, %s", existingName, newName)
	}

	if _, err := tx.Stmt(u.parent.renameMbox).Exec(newName, u.id, existingName); err != nil {
		return errors.Wrapf(err, "RenameMailbox %s, %s", existingName, newName)
	}

	// TODO: Check if it possible to merge these queries.
	existingPattern := existingName + MailboxPathSep + "%"
	newPrefix := newName + MailboxPathSep
	existingPrefixLen := len(existingName + MailboxPathSep)
	if _, err := tx.Stmt(u.parent.renameMboxChilds).Exec(newPrefix, existingPrefixLen, existingPattern, u.id); err != nil {
		return errors.Wrapf(err, "RenameMailbox (childs) %s, %s", existingName, newName)
	}

	if strings.EqualFold(existingName, "INBOX") {
		if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, existingName, u.parent.prng.Uint32(), nil); err != nil {
			return errors.Wrapf(err, "RenameMailbox %s, %s", existingName, newName)
		}

		// TODO: Cut a query here by using RETURNING on PostgreSQL
		var inboxId uint64
		if err = tx.Stmt(u.parent.mboxId).QueryRow(u.id, "INBOX").Scan(&inboxId); err != nil {
			return errors.Wrap(err, "CreateUser")
		}
		if _, err := tx.Stmt(u.parent.setInboxId).Exec(inboxId, u.id); err != nil {
			return errors.Wrapf(err, "RenameMailbox %s, %s", existingName, newName)
		}
	}

	return errors.Wrapf(tx.Commit(), "RenameMailbox %s, %s", existingName, newName)
}

func (u *User) Logout() error {
	return nil
}

func (u *User) createParentDirs(tx *sql.Tx, name string) error {
	parts := strings.Split(name, MailboxPathSep)
	curDir := ""
	for i, part := range parts[:len(parts)-1] {
		if i != 0 {
			curDir += MailboxPathSep
		}
		curDir += part

		if _, err := tx.Stmt(u.parent.createMboxExistsOk).Exec(u.id, curDir, u.parent.prng.Uint32()); err != nil {
			return err
		}
	}
	return nil
}
