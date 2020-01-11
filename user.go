package imapsql

import (
	"database/sql"
	"strings"

	"errors"

	specialuse "github.com/emersion/go-imap-specialuse"
	"github.com/emersion/go-imap/backend"
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
		u.parent.logUserErr(u, err, "ListMailboxes", subscribed)
		return nil, wrapErr(err, "ListMailboxes")
	}
	defer rows.Close()

	res := []backend.Mailbox{}
	for rows.Next() {
		id, name := uint64(0), ""
		if err := rows.Scan(&id, &name); err != nil {
			u.parent.logUserErr(u, err, "ListMailboxes", subscribed)
			return nil, wrapErr(err, "ListMailboxes")
		}

		res = append(res, &Mailbox{user: *u, id: id, name: name, parent: u.parent})
	}
	if err := rows.Err(); err != nil {
		u.parent.logUserErr(u, err, "ListMailboxes", subscribed)
		return res, wrapErr(rows.Err(), "ListMailboxes")
	}
	return res, nil
}

func (u *User) GetMailbox(name string) (backend.Mailbox, error) {
	if strings.EqualFold(name, "INBOX") {
		return &Mailbox{user: *u, id: u.inboxId, name: name, parent: u.parent}, nil
	}

	row := u.parent.mboxId.QueryRow(u.id, name)
	id := uint64(0)
	if err := row.Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return nil, backend.ErrNoSuchMailbox
		}
		u.parent.logUserErr(u, err, "GetMailbox", name)
		return nil, wrapErrf(err, "GetMailbox %s", name)
	}

	return &Mailbox{user: *u, id: id, name: name, parent: u.parent}, nil
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
		u.parent.logUserErr(u, err, "CreateMailbox (tx start)", name)
		return wrapErrf(err, "CreateMailbox %s", name)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := u.createParentDirs(tx, name); err != nil {
		u.parent.logUserErr(u, err, "CreateMailbox (parents)", name)
		return wrapErrf(err, "CreateMailbox (parents) %s", name)
	}

	if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, name, u.parent.prng.Uint32(), nil); err != nil {
		if isForeignKeyErr(err) {
			return backend.ErrMailboxAlreadyExists
		}
		u.parent.logUserErr(u, err, "CreateMailbox", name)
		return wrapErrf(err, "CreateMailbox %s", name)
	}

	err = tx.Commit()
	u.parent.logUserErr(u, err, "CreateMailbox (tx commit)", name)
	return wrapErrf(err, "CreateMailbox (tx commit) %s", name)
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
		return wrapErrf(err, "CreateMailboxSpecial %s", name)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := u.createParentDirs(tx, name); err != nil {
		return wrapErrf(err, "CreateMailboxSpecial (parents) %s", name)
	}

	if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, name, u.parent.prng.Uint32(), specialUseAttr); err != nil {
		if isForeignKeyErr(err) {
			return backend.ErrMailboxAlreadyExists
		}
		return wrapErrf(err, "CreateMailboxSpecial %s", name)
	}

	return wrapErrf(tx.Commit(), "CreateMailbox (tx commit) %s", name)
}

func (u *User) DeleteMailbox(name string) error {
	if strings.ToLower(name) == "inbox" {
		return errors.New("DeleteMailbox: can't delete INBOX")
	}

	tx, err := u.parent.db.BeginLevel(sql.LevelRepeatableRead, false)
	if err != nil {
		u.parent.logUserErr(u, err, "DeleteMailbox (tx start)", name)
		return wrapErrf(err, "DeleteMailbox %s", name)
	}
	defer tx.Rollback()

	if _, err := tx.Stmt(u.parent.decreaseRefForMbox).Exec(u.id, name); err != nil {
		u.parent.logUserErr(u, err, "DeleteMailbox (decrease ref)", name)
		return wrapErrf(err, "DeleteMailbox %s", name)
	}

	rows, err := tx.Stmt(u.parent.zeroRefUser).Query(u.id)
	if err != nil {
		u.parent.logUserErr(u, err, "DeleteMailbox (zero ref user)", name)
		return wrapErrf(err, "DeleteMailbox %s", name)
	}
	defer rows.Close()

	keys := make([]string, 0, 16)
	for rows.Next() {
		var extKey string
		if err := rows.Scan(&extKey); err != nil {
			u.parent.logUserErr(u, err, "DeleteMailbox (extkeys scan)", name)
			return wrapErrf(err, "DeleteMailbox %s", name)
		}
		keys = append(keys, extKey)

	}

	if err := u.parent.extStore.Delete(keys); err != nil {
		u.parent.logUserErr(u, err, "DeleteMailbox (extstore delete)", name)
		return wrapErrf(err, "DeleteMailbox %s", name)
	}

	// TODO: Grab mboxId along the way on PostgreSQL?
	stats, err := tx.Stmt(u.parent.deleteMbox).Exec(u.id, name)
	if err != nil {
		u.parent.logUserErr(u, err, "DeleteMailbox (delete mbox)", name)
		return wrapErrf(err, "DeleteMailbox %s", name)
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		u.parent.logUserErr(u, err, "DeleteMailbox (stats)", name)
		return wrapErrf(err, "DeleteMailbox %s", name)
	}
	if affected == 0 {
		return backend.ErrNoSuchMailbox
	}

	if _, err := tx.Stmt(u.parent.deleteZeroRef).Exec(u.id); err != nil {
		u.parent.logUserErr(u, err, "DeleteMailbox (delete zero ref)", name)
		return wrapErrf(err, "DeleteMailbox %s", name)
	}

	err = tx.Commit()
	u.parent.logUserErr(u, err, "DeleteMailbox (tx commit)", name)
	return err
}

func (u *User) RenameMailbox(existingName, newName string) error {
	tx, err := u.parent.db.Begin(false)
	if err != nil {
		u.parent.logUserErr(u, err, "RenameMailbox (tx start)", existingName, newName)
		return wrapErrf(err, "RenameMailbox %s, %s", existingName, newName)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := u.createParentDirs(tx, newName); err != nil {
		u.parent.logUserErr(u, err, "RenameMailbox (create parents)", existingName, newName)
		return wrapErrf(err, "RenameMailbox %s, %s", existingName, newName)
	}

	if _, err := tx.Stmt(u.parent.renameMbox).Exec(newName, u.id, existingName); err != nil {
		u.parent.logUserErr(u, err, "RenameMailbox", existingName, newName)
		return wrapErrf(err, "RenameMailbox %s, %s", existingName, newName)
	}

	// TODO: Check if it possible to merge these queries.
	existingPattern := existingName + MailboxPathSep + "%"
	newPrefix := newName + MailboxPathSep
	existingPrefixLen := len(existingName + MailboxPathSep)
	if _, err := tx.Stmt(u.parent.renameMboxChilds).Exec(newPrefix, existingPrefixLen, existingPattern, u.id); err != nil {
		u.parent.logUserErr(u, err, "RenameMailbox (childs)", existingName, newName)
		return wrapErrf(err, "RenameMailbox (childs) %s, %s", existingName, newName)
	}

	if strings.EqualFold(existingName, "INBOX") {
		if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, existingName, u.parent.prng.Uint32(), nil); err != nil {
			u.parent.logUserErr(u, err, "RenameMailbox (create inbox)", existingName, newName)
			return wrapErrf(err, "RenameMailbox %s, %s", existingName, newName)
		}

		// TODO: Cut a query here by using RETURNING on PostgreSQL
		var inboxId uint64
		if err = tx.Stmt(u.parent.mboxId).QueryRow(u.id, "INBOX").Scan(&inboxId); err != nil {
			u.parent.logUserErr(u, err, "RenameMailbox (query mboxid id)", existingName, newName)
			return wrapErrf(err, "RenameMailbox %s, %s", existingName, newName)
		}
		if _, err := tx.Stmt(u.parent.setInboxId).Exec(inboxId, u.id); err != nil {
			u.parent.logUserErr(u, err, "RenameMailbox (set inbox id)", existingName, newName)
			return wrapErrf(err, "RenameMailbox %s, %s", existingName, newName)
		}
	}

	err = tx.Commit()
	u.parent.logUserErr(u, err, "RenameMailbox (tx commit)", existingName, newName)
	return wrapErrf(err, "RenameMailbox %s, %s", existingName, newName)
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
