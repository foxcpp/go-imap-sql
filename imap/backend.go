package imap

import (
	"database/sql"
	"strings"

	"github.com/emersion/go-imap/backend"
	"github.com/pkg/errors"
)

type Backend struct {
	db *sql.DB

	driver string

	// Shitton of pre-compiled SQL statements.
	userId             *sql.Stmt
	listMboxes         *sql.Stmt
	listSubbedMboxes   *sql.Stmt
	createMboxExistsOk *sql.Stmt
	createMbox         *sql.Stmt
	deleteMbox         *sql.Stmt
	renameMbox         *sql.Stmt
	getMboxMark        *sql.Stmt
	setSubbed          *sql.Stmt
	uidNext            *sql.Stmt
	uidValidity        *sql.Stmt
	msgsCount          *sql.Stmt
	recentCount        *sql.Stmt
	firstUnseenSeqNum  *sql.Stmt
	expungeMbox        *sql.Stmt
	mboxId             *sql.Stmt
	addMsg             *sql.Stmt
	copyMsgsUid        *sql.Stmt
	copyMsgFlagsUid    *sql.Stmt
	copyMsgsSeq        *sql.Stmt
	copyMsgFlagsSeq    *sql.Stmt
	getMsgsBodyUid     *sql.Stmt
	getMsgsBodySeq     *sql.Stmt
	getMsgsNoBodyUid   *sql.Stmt
	getMsgsNoBodySeq   *sql.Stmt
	massClearFlagsUid  *sql.Stmt
	massClearFlagsSeq  *sql.Stmt
}

func NewBackend(driver, dsn string) (*Backend, error) {
	b := new(Backend)
	var err error

	if driver == "sqlite3" {
		if !strings.HasPrefix(dsn, "file:") {
			dsn = "file:" + dsn
		}
		if !strings.Contains(dsn, "?") {
			dsn = dsn + "?"
		}

		dsn = dsn + "_journal=WAL&_busy_timeout=5000"
	}

	b.driver = driver

	b.db, err = sql.Open(driver, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}

	if driver == "sqlite3" {
		b.db.Exec(`PRAGMA foreign_keys = ON`)
	}

	if err := b.initSchema(); err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}
	if err := b.prepareStmts(); err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}

	return b, nil
}

func (b *Backend) Close() error {
	return b.db.Close()
}

func (b *Backend) initSchema() error {
	var err error
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER NOT NULL PRIMARY KEY,
			username VARCHAR(255) NOT NULL UNIQUE,
			password VARCHAR(255) DEFAULT NULL,
			password_salt BLOB DEFAULT NULL
		)`)
	if err != nil {
		return errors.Wrap(err, "create table users")
	}
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS mboxes (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			uid INTEGER NOT NULL REFERENCES users(id),
			name VARCHAR(255) NOT NULL,
			sub INTEGER NOT NULL DEFAULT 0,
			mark INTEGER NOT NULL DEFAULT 0,
			uidvalidity INTEGER NOT NULL,

			UNIQUE(uid, name)
		)`)
	if err != nil {
		return errors.Wrap(err, "create table mboxes")
	}
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS msgs (
			mboxId INTEGER NOT NULL REFERENCES mboxes(id) ON DELETE CASCADE,
			msgId INTEGER NOT NULL,
			date INTEGER NOT NULL,
			bodyLen INTEGER NOT NULL,
			body BLOB NOT NULL,

			PRIMARY KEY(mboxId, msgId)
		)`)
	if err != nil {
		return errors.Wrap(err, "create table msgs")
	}
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS flags (
			mboxId INTEGER NOT NULL,
			msgId INTEGER NOT NULL,
			flag VARCHAR(255) NOT NULL,

			FOREIGN KEY (mboxId, msgId) REFERENCES msgs(mboxId, msgId) ON DELETE CASCADE,
			UNIQUE(mboxId, msgId, flag)
		)`)
	if err != nil {
		return errors.Wrap(err, "create table flags")
	}
	return nil
}

func (b *Backend) prepareStmts() error {
	var err error

	b.userId, err = b.db.Prepare(`
		SELECT id
		FROM users
		WHERE username = ?`)
	if err != nil {
		return errors.Wrap(err, "userId prep")
	}
	b.listMboxes, err = b.db.Prepare(`
		SELECT id, name
		FROM mboxes
		WHERE uid = ?`)
	if err != nil {
		return errors.Wrap(err, "listMboxes prep")
	}
	b.listSubbedMboxes, err = b.db.Prepare(`
		SELECT id, name
		FROM mboxes
		WHERE uid = ? AND sub = 1`)
	if err != nil {
		return errors.Wrap(err, "listSubbedMboxes prep")
	}
	b.createMbox, err = b.db.Prepare(`
		INSERT INTO mboxes(uid, name, uidvalidity)
		VALUES (?, ?, ?)`)
	if err != nil {
		return errors.Wrap(err, "createMbox prep")
	}
	b.createMboxExistsOk, err = b.db.Prepare(`
		INSERT INTO mboxes(uid, name, uidvalidity)
		VALUES (?, ?, ?) ON CONFLICT DO NOTHING`)
	if err != nil {
		return errors.Wrap(err, "createMboxExistsOk prep")
	}
	b.deleteMbox, err = b.db.Prepare(`
		DELETE FROM mboxes
		WHERE uid = ? AND name = ?`)
	if err != nil {
		return errors.Wrap(err, "deleteMbox prep")
	}
	b.renameMbox, err = b.db.Prepare(`
		UPDATE mboxes SET name = ?
		WHERE uid = ? AND name = ?`)
	if err != nil {
		return errors.Wrap(err, "renameMbox prep")
	}
	b.getMboxMark, err = b.db.Prepare(`
		SELECT mark FROM mboxes
		WHERE uid = ? AND name = ?`)
	if err != nil {
		return errors.Wrap(err, "getMboxMark prep")
	}
	b.setSubbed, err = b.db.Prepare(`
		UPDATE mboxes SET sub = ?
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "setSubbed prep")
	}
	b.uidNext, err = b.db.Prepare(`
		SELECT max(msgId)+1
		FROM msgs
		WHERE mboxId = ?`)
	if err != nil {
		return errors.Wrap(err, "uidNext prep")
	}
	b.uidValidity, err = b.db.Prepare(`
		SELECT uidvalidity
		FROM mboxes
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "uidvalidity prep")
	}
	b.msgsCount, err = b.db.Prepare(`
		SELECT count()
		FROM msgs
		WHERE mboxId = ?`)
	if err != nil {
		return errors.Wrap(err, "msgsCount prep")
	}
	b.recentCount, err = b.db.Prepare(`
		SELECT count()
		FROM flags
		WHERE mboxId = ? AND flag = '\Recent'`)
	if err != nil {
		return errors.Wrap(err, "recentCount prep")
	}
	// TODO: This query is kinda expensive, consider moving
	// flags with special semantics (Recent, Seen, Deleted) to
	// msgs table as columns.
	b.firstUnseenSeqNum, err = b.db.Prepare(`
		SELECT rownr
		FROM (
			SELECT row_number() OVER (ORDER BY msgId) AS rownr, msgId
			FROM msgs
			WHERE mboxId = ?
		)
		WHERE msgId NOT IN (
			SELECT msgId
			FROM flags
			WHERE mboxId = ?
			AND flag = '\Seen'
		)`)
	if err != nil {
		return errors.Wrap(err, "firstUnseenSeqNum prep")
	}
	b.expungeMbox, err = b.db.Prepare(`
		DELETE FROM msgs
		WHERE mboxId = ? AND msgId IN (
			SELECT msgId
			FROM flags
			WHERE mboxId = ?
			AND flag = '\Deleted'
		)`)
	if err != nil {
		return errors.Wrap(err, "expungeMbox prep")
	}
	b.mboxId, err = b.db.Prepare(`
		SELECT id FROM mboxes
		WHERE uid = ?
		AND name = ?`)
	if err != nil {
		return errors.Wrap(err, "mboxId prep")
	}
	b.addMsg, err = b.db.Prepare(`
		INSERT INTO msgs(mboxId, msgId, date, bodyLen, body)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return errors.Wrap(err, "addMsg prep")
	}
	b.copyMsgsUid, err = b.db.Prepare(`
		INSERT INTO msgs
		SELECT ? AS mboxId, coalesce((
			SELECT max(msgId)
			FROM msgs
			WHERE mboxId = ?
		), 0) + row_number() OVER (ORDER BY msgId) AS msgId, date, bodyLen, body
		FROM msgs
		WHERE mboxId = ? AND msgId BETWEEN ? AND ?`)
	if err != nil {
		return errors.Wrap(err, "copyMsgsUid prep")
	}
	b.copyMsgFlagsUid, err = b.db.Prepare(`
		INSERT INTO flags
		SELECT ?, new_msgId AS msgId, flags.flag FROM flags
		INNER JOIN (
			SELECT DISTINCT coalesce((
				SELECT max(msgId) - 1
				FROM msgs
				WHERE mboxId = ?
			), 0) + row_number() OVER (ORDER BY msgId) AS new_msgId, msgId, flag
			FROM flags WHERE mboxId = ? AND msgId BETWEEN ? AND ?
		) map ON map.msgId = flags.msgId AND map.flag = flags.flag
		WHERE mboxId = ?`)
	if err != nil {
		return errors.Wrap(err, "copyMsgFlagsUid prep")
	}
	b.copyMsgsSeq, err = b.db.Prepare(`
		INSERT INTO msgs
		SELECT ? AS mboxId, coalesce((
			SELECT max(msgId)
			FROM msgs
			WHERE mboxId = ?
		), 0) + row_number() OVER (ORDER BY msgId) AS msgId, date, bodyLen, body
		FROM msgs
		WHERE mboxId = ? LIMIT ? OFFSET ?`)
	if err != nil {
		return errors.Wrap(err, "copyMsgsSeq prep")
	}
	b.copyMsgFlagsSeq, err = b.db.Prepare(`
		INSERT INTO flags
		SELECT ?, new_msgId AS msgId, flags.flag FROM flags
		INNER JOIN (
			SELECT DISTINCT coalesce((
				SELECT max(msgId) - 1
				FROM msgs
				WHERE mboxId = ?
			), 0) + row_number() OVER (ORDER BY msgId) AS  new_msgId, msgId, flag
			FROM flags WHERE mboxId = ? LIMIT ? OFFSET ?
		) map ON map.msgId = flags.msgId AND map.flag = flags.flag
		WHERE mboxId = ?`)
	if err != nil {
		return errors.Wrap(err, "copyMsgFlagsSeq prep")
	}
	b.getMsgsNoBodyUid, err = b.db.Prepare(`
		SELECT DISTINCT seqnum, msgs.msgId, date, bodyLen, NULL, coalesce(` + b.groupConcatFn("flag", "{") + `, "")
		FROM msgs
		INNER JOIN (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
		) map
		ON map.msgId = msgs.msgId
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId
		WHERE msgs.mboxId = ? AND msgs.msgId BETWEEN ? AND ?
		GROUP BY msgs.mboxId, msgs.msgId`)
	if err != nil {
		return errors.Wrap(err, "getMsgsNoBodyUid prep")
	}
	b.getMsgsBodyUid, err = b.db.Prepare(`
		SELECT DISTINCT seqnum, msgs.msgId, date, bodyLen, body, coalesce(` + b.groupConcatFn("flag", "{") + `, "")
		FROM msgs
		INNER JOIN (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
		) map
		ON map.msgId = msgs.msgId
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId
		WHERE msgs.mboxId = ? AND msgs.msgId BETWEEN ? AND ?
		GROUP BY msgs.mboxId, msgs.msgId`)
	if err != nil {
		return errors.Wrap(err, "getMsgsBodyUid prep")
	}
	b.getMsgsNoBodySeq, err = b.db.Prepare(`
		SELECT DISTINCT seqnum, msgs.msgId, date, bodyLen, NULL, coalesce(` + b.groupConcatFn("flag", "{") + `, "")
		FROM msgs
		INNER JOIN (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
		) map
		ON map.msgId = msgs.msgId
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId
		WHERE msgs.mboxId = ?
		GROUP BY msgs.mboxId, msgs.msgId
		LIMIT ? OFFSET ?-1
		`)
	if err != nil {
		return errors.Wrap(err, "getMsgsNoBodySeq prep")
	}
	b.getMsgsBodySeq, err = b.db.Prepare(`
		SELECT DISTINCT seqnum, msgs.msgId, date, bodyLen, body, coalesce(` + b.groupConcatFn("flag", "{") + `, "")
		FROM msgs
		INNER JOIN (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
		) map
		ON map.msgId = msgs.msgId
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId
		WHERE msgs.mboxId = ?
		GROUP BY msgs.mboxId, msgs.msgId
		LIMIT ? OFFSET ?-1`)
	if err != nil {
		return errors.Wrap(err, "getMsgsBodySeq prep")
	}
	b.massClearFlagsUid, err = b.db.Prepare(`
		DELETE FROM flags
		WHERE mboxId = ?
		AND msgId BETWEEN ? AND ?`)
	if err != nil {
		return errors.Wrap(err, "massClearFlagsUid prep")
	}
	b.massClearFlagsSeq, err = b.db.Prepare(`
		DELETE FROM flags
		WHERE mboxId = ?
		AND msgId IN (
			SELECT msgId
			FROM msgs
			WHERE mboxId = ?
			LIMIT ? OFFSET ?
		)`)
	if err != nil {
		return errors.Wrap(err, "massClearFlagsSeq prep")
	}

	return nil
}

func (b *Backend) groupConcatFn(expr, separator string) string {
	if b.driver == "sqlite3" {
		return "group_concat(" + expr + ", '" + separator + "')"
	} else {
		return "group_concat(" + expr + " SEPARATOR'" + separator + "')"
	}
}

func (b *Backend) UserID(username string) (uint64, error) {
	row := b.userId.QueryRow(username)
	id := uint64(0)
	return id, row.Scan(&id)
}

// Login is a dummy method, as go-sqlmail itself doesn't implements user
// account management and this is responsibility of backend stacked on top of
// it.
//
// Despite of this, go-sqlmail creates table `users` with username->numeric ID
// mapping and requires this table to be populated with usernames of
// "registered" users. Usernames are not used directly by go-sqlmail for
// performance reasons.
//
// This table also contains password and password_salt columns that may be used
// to store actual passwords by auth backend, but they are unused by backend
// itself.
func (b *Backend) Login(username, password string) (backend.User, error) {
	uid, err := b.UserID(username)
	if err != nil {
		return nil, backend.ErrInvalidCredentials
	}

	return &User{id: uid, username: username, parent: b}, nil
}
