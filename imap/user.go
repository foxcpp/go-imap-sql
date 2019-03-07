package imap

import (
	"database/sql"
	"strings"
	"time"

	"github.com/emersion/go-imap/backend"
	"github.com/pkg/errors"
)

const MailboxPathSep = "."

type User struct {
	id       uint64
	username string
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
	tx, err := u.parent.db.Begin()
	if err != nil {
		return errors.Wrapf(err, "CreateMailbox %s", name)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := u.createParentDirs(tx, name); err != nil {
		return errors.Wrapf(err, "CreateMailbox (parents) %s", name)
	}

	if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, name, time.Now().Unix()); err != nil {
		if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Duplicate entry") { // TODO: Check error messages for other RDBMS.
			return backend.ErrMailboxAlreadyExists
		}
		return errors.Wrapf(err, "CreateMailbox %s", name)
	}
	return errors.Wrapf(tx.Commit(), "CreateMailbox %s", name)
}

func (u *User) DeleteMailbox(name string) error {
	if strings.ToLower(name) == "inbox" {
		return errors.New("DeleteMailbox: can't delete INBOX")
	}
	if stats, err := u.parent.deleteMbox.Exec(u.id, name); err != nil {
		return errors.Wrapf(err, "DeleteMailbox %s", name)
	} else {
		affected, err := stats.RowsAffected()
		if err != nil {
			return errors.Wrapf(err, "DeleteMailbox %s", name)
		}
		if affected == 0 {
			return backend.ErrNoSuchMailbox
		}
	}

	return nil
}

func (u *User) RenameMailbox(existingName, newName string) error {
	tx, err := u.parent.db.Begin()
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

	existingPattern := existingName + MailboxPathSep + "%"
	newPrefix := newName + MailboxPathSep
	existingPrefixLen := len(existingName + MailboxPathSep)
	if _, err := tx.Stmt(u.parent.renameMboxChilds).Exec(newPrefix, existingPrefixLen, existingPattern, u.id); err != nil {
		return errors.Wrapf(err, "RenameMailbox (childs) %s, %s", existingName, newName)
	}

	if existingName == "INBOX" {
		if _, err := tx.Stmt(u.parent.createMbox).Exec(u.id, existingName, time.Now().Unix()); err != nil {
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

		if _, err := tx.Stmt(u.parent.createMboxExistsOk).Exec(u.id, curDir, time.Now().Unix()); err != nil {
			return err
		}
	}
	return nil
}
