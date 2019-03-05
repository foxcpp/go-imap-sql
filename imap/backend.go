package imap

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/emersion/go-imap/backend"
	"github.com/foxcpp/go-sqlmail"
	"github.com/pkg/errors"
	"golang.org/x/crypto/sha3"
)

// db struct is a thin wrapper to solve the most annoying problems
// with cross-RDBMS compatibility.
type db struct {
	DB      *sql.DB
	driver  string
	mysql57 bool
}

func (d db) Prepare(req string) (*sql.Stmt, error) {
	return d.DB.Prepare(d.rewriteSQL(req))
}

func (d db) Query(req string, args ...interface{}) (*sql.Rows, error) {
	return d.DB.Query(d.rewriteSQL(req), args...)
}

func (d db) QueryRow(req string, args ...interface{}) *sql.Row {
	return d.DB.QueryRow(d.rewriteSQL(req), args...)
}

func (d db) Exec(req string, args ...interface{}) (sql.Result, error) {
	return d.DB.Exec(d.rewriteSQL(req), args...)
}

func (d db) Begin() (*sql.Tx, error) {
	return d.DB.Begin()
}

func (d db) Close() error {
	return d.DB.Close()
}

func (d db) rewriteSQL(req string) (res string) {
	res = strings.TrimSpace(req)
	res = strings.TrimLeft(res, "\n\t")
	if d.driver == "postgres" {
		res = ""
		placeholderIndx := 1
		for _, chr := range req {
			if chr == '?' {
				res += "$" + strconv.Itoa(placeholderIndx)
				placeholderIndx += 1
			} else {
				res += string(chr)
			}
		}
		res = strings.TrimLeft(res, "\n\t")
		if strings.HasPrefix(res, "CREATE TABLE") {
			res = strings.Replace(res, "BLOB", "BYTEA", -1)
			res = strings.Replace(res, "AUTOINCREMENT", "", -1)
		}
	} else if d.driver == "mysql" {
		if strings.HasPrefix(res, "CREATE TABLE") {
			res = strings.Replace(res, "BIGSERIAL", "BIGINT", -1)
			res = strings.Replace(res, "AUTOINCREMENT", "AUTO_INCREMENT", -1)
		}
		if strings.HasSuffix(res, "ON CONFLICT DO NOTHING") && strings.HasPrefix(res, "INSERT") {
			res = strings.Replace(res, "ON CONFLICT DO NOTHING", "", -1)
			res = strings.Replace(res, "INSERT", "INSERT IGNORE", 1)
		}
	} else if d.driver == "sqlite3" {
		if strings.HasPrefix(res, "CREATE TABLE") {
			res = strings.Replace(res, "BIGSERIAL", "INTEGER", -1)
		}
	}
	return
}

type Backend struct {
	db db

	driver string

	// Shitton of pre-compiled SQL statements.
	userCreds          *sql.Stmt
	addUser            *sql.Stmt
	delUser            *sql.Stmt
	setUserPass        *sql.Stmt
	listMboxes         *sql.Stmt
	listSubbedMboxes   *sql.Stmt
	createMboxExistsOk *sql.Stmt
	createMbox         *sql.Stmt
	deleteMbox         *sql.Stmt
	renameMbox         *sql.Stmt
	renameMboxChilds   *sql.Stmt
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
	b.db.driver = driver

	b.db.DB, err = sql.Open(driver, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}

	if driver == "sqlite3" {
		_, err := b.db.Exec(`PRAGMA foreign_keys = ON`)
		if err != nil {
			return nil, errors.Wrap(err, "NewBackend")
		}
	} else if driver == "mysql" {
		_, err := b.db.Exec(`SET SESSION sql_mode = 'ansi,no_backslash_escapes'`)
		if err != nil {
			return nil, errors.Wrap(err, "NewBackend")
		}
		_, err = b.db.Exec(`SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE`)
		if err != nil {
			return nil, errors.Wrap(err, "NewBackend")
		}
		row := b.db.QueryRow(`SELECT version()`)
		mysqlVer := ""
		if err := row.Scan(&mysqlVer); err != nil {
			return nil, errors.Wrap(err, "NewBackend")
		}
		b.db.mysql57 = strings.HasPrefix(mysqlVer, "5.7.")
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
			id BIGSERIAL NOT NULL PRIMARY KEY AUTOINCREMENT,
			username VARCHAR(255) NOT NULL UNIQUE,
			password VARCHAR(255) DEFAULT NULL,
			password_salt VARCHAR(255) DEFAULT NULL
		)`)
	if err != nil {
		return errors.Wrap(err, "create table users")
	}
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS mboxes (
			id BIGSERIAL NOT NULL PRIMARY KEY AUTOINCREMENT,
			uid INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
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
			mboxId BIGINT NOT NULL REFERENCES mboxes(id) ON DELETE CASCADE,
			msgId BIGINT NOT NULL,
			date BIGINT NOT NULL,
			bodyLen INTEGER NOT NULL,
			body BLOB NOT NULL,

			PRIMARY KEY(mboxId, msgId)
		)`)
	if err != nil {
		return errors.Wrap(err, "create table msgs")
	}
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS flags (
			mboxId BIGINT NOT NULL,
			msgId BIGINT NOT NULL,
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

	b.userCreds, err = b.db.Prepare(`
		SELECT id, password, password_salt
		FROM users
		WHERE username = ?`)
	if err != nil {
		return errors.Wrap(err, "userId prep")
	}
	b.addUser, err = b.db.Prepare(`
		INSERT INTO users(username, password, password_salt)
		VALUES (?, ?, ?)`)
	if err != nil {
		return errors.Wrap(err, "addUser prep")
	}
	b.delUser, err = b.db.Prepare(`
		DELETE FROM users
		WHERE username = ?`)
	if err != nil {
		return errors.Wrap(err, "addUser prep")
	}
	b.setUserPass, err = b.db.Prepare(`
		UPDATE users
		SET password = ?, password_salt = ?
		WHERE username = ?`)
	if err != nil {
		return errors.Wrap(err, "addUser prep")
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
	if b.db.driver == "mysql" {
		b.renameMboxChilds, err = b.db.Prepare(`
		UPDATE mboxes SET name = concat(?, substr(name, ?+1))
		WHERE name LIKE ? AND uid = ?`)
	} else {
		b.renameMboxChilds, err = b.db.Prepare(`
		UPDATE mboxes SET name = ? || substr(name, ?+1)
		WHERE name LIKE ? AND uid = ?`)
	}
	if err != nil {
		return errors.Wrap(err, "renameMboxChilds prep")
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
		SELECT count(*)
		FROM msgs
		WHERE mboxId = ?`)
	if err != nil {
		return errors.Wrap(err, "msgsCount prep")
	}
	b.recentCount, err = b.db.Prepare(`
		SELECT count(*)
		FROM flags
		WHERE mboxId = ? AND flag = '\Recent'`)
	if err != nil {
		return errors.Wrap(err, "recentCount prep")
	}
	// TODO: This query is kinda expensive, consider moving
	// flags with special semantics (Recent, Seen, Deleted) to
	// msgs table as columns.
	if b.db.mysql57 {
		// MySQL 5.7 doesn't have row_number() function.
		b.firstUnseenSeqNum, err = b.db.Prepare(`
		SELECT rownr
		FROM (
			SELECT (@rownum:=@rownum + 1) AS rownr, msgId
			FROM msgs, (SELECT @rownum := 0) counter
			WHERE mboxId = ?
			ORDER BY msgId
		) seqnum
		WHERE msgId NOT IN (
			SELECT msgId
			FROM flags
			WHERE mboxId = ?
			AND flag = '\Seen'
		)`)
	} else {
		b.firstUnseenSeqNum, err = b.db.Prepare(`
			SELECT rownr
			FROM (
				SELECT row_number() OVER (ORDER BY msgId) AS rownr, msgId
				FROM msgs
				WHERE mboxId = ?
			) seqnum
			WHERE msgId NOT IN (
				SELECT msgId
				FROM flags
				WHERE mboxId = ?
				AND flag = '\Seen'
			)`)
	}
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
	if b.db.mysql57 {
		b.copyMsgsUid, err = b.db.Prepare(`
			INSERT INTO msgs
			SELECT ? AS mboxId, coalesce((
				SELECT max(msgId)
				FROM msgs
				WHERE mboxId = ?
			), 0) + (@rownum := @rownum + 1), date, bodyLen, body
			FROM msgs, (SELECT @rownum := 0) counter
			WHERE mboxId = ? AND msgId BETWEEN ? AND ?
			ORDER BY msgId`)
	} else {
		b.copyMsgsUid, err = b.db.Prepare(`
			INSERT INTO msgs
			SELECT ? AS mboxId, coalesce((
				SELECT max(msgId)
				FROM msgs
				WHERE mboxId = ?
			), 0) + row_number() OVER (ORDER BY msgId), date, bodyLen, body
			FROM msgs
			WHERE mboxId = ? AND msgId BETWEEN ? AND ?`)
	}
	if err != nil {
		return errors.Wrap(err, "copyMsgsUid prep")
	}
	if b.db.mysql57 {
		b.copyMsgFlagsUid, err = b.db.Prepare(`
			INSERT INTO flags
			SELECT ?, new_msgId AS msgId, flag
			FROM flags
			INNER JOIN (
				SELECT coalesce((
					SELECT max(msgId) - ?
					FROM msgs
					WHERE mboxId = ?
				), 0) + (@rownum := @rownum + 1) AS new_msgId, msgId, mboxId
				FROM msgs, (SELECT @rownum := 0) counter
				WHERE mboxId = ?
				AND msgId BETWEEN ? AND ?
				ORDER BY msgId
			) map ON map.msgId = flags.msgId
			AND map.mboxId = flags.mboxId`)
	} else {
		b.copyMsgFlagsUid, err = b.db.Prepare(`
			INSERT INTO flags
			SELECT ?, new_msgId AS msgId, flag
			FROM flags
			INNER JOIN (
				SELECT coalesce((
					SELECT max(msgId) - ?
					FROM msgs
					WHERE mboxId = ?
				), 0) + row_number() OVER (ORDER BY msgId) AS new_msgId, msgId, mboxId
				FROM msgs
				WHERE mboxId = ?
				AND msgId BETWEEN ? AND ?
			) map ON map.msgId = flags.msgId
			AND map.mboxId = flags.mboxId`)
	}
	if err != nil {
		return errors.Wrap(err, "copyMsgFlagsUid prep")
	}
	if b.db.mysql57 {
		b.copyMsgsSeq, err = b.db.Prepare(`
			INSERT INTO msgs
			SELECT ? AS mboxId, coalesce((
				SELECT max(msgId)
				FROM msgs
				WHERE mboxId = ?
			), 0) + (@rownum := @rownum + 1), date, bodyLen, body
			FROM (
				SELECT msgId, date, bodyLen, body
				FROM msgs
				WHERE mboxId = ?
				LIMIT ? OFFSET ?
			) subset, (SELECT @rownum := 0) counter
			ORDER BY msgId`)
	} else {
		b.copyMsgsSeq, err = b.db.Prepare(`
			INSERT INTO msgs
			SELECT ? AS mboxId, coalesce((
				SELECT max(msgId)
				FROM msgs
				WHERE mboxId = ?
			), 0) + row_number() OVER (ORDER BY msgId), date, bodyLen, body
			FROM (
				SELECT msgId, date, bodyLen, body
				FROM msgs
				WHERE mboxId = ?
				LIMIT ? OFFSET ?
			) subset`)
	}
	if err != nil {
		return errors.Wrap(err, "copyMsgsSeq prep")
	}
	if b.db.mysql57 {
		b.copyMsgFlagsSeq, err = b.db.Prepare(`
			INSERT INTO flags
			SELECT ?, new_msgId AS msgId, flag
			FROM flags
			INNER JOIN (
				SELECT coalesce((
					SELECT max(msgId) - ?
					FROM msgs
					WHERE mboxId = ?
				), 0) + (@rownum := @rownum + 1) AS new_msgId, msgId
				FROM (
					SELECT msgId
					FROM msgs
					WHERE mboxId = ?
					LIMIT ? OFFSET ?
				) subset, (SELECT @rownum := 0) counter
			) map ON map.msgId = flags.msgId`)
	} else {
		b.copyMsgFlagsSeq, err = b.db.Prepare(`
			INSERT INTO flags
			SELECT ?, new_msgId AS msgId, flag
			FROM flags
			INNER JOIN (
				SELECT coalesce((
					SELECT max(msgId) - ?
					FROM msgs
					WHERE mboxId = ?
				), 0) + row_number() OVER (ORDER BY msgId) AS new_msgId, msgId
				FROM (
					SELECT msgId
					FROM msgs
					WHERE mboxId = ?
					LIMIT ? OFFSET ?
				) subset
			) map ON map.msgId = flags.msgId`)
	}
	if err != nil {
		return errors.Wrap(err, "copyMsgFlagsSeq prep")
	}
	if b.db.mysql57 {
		b.getMsgsNoBodyUid, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, NULL, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT (@rownum := @rownum + 1) AS seqnum, msgId, mboxId
				FROM msgs, (SELECT @rownum := 0) counter
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
			WHERE msgs.mboxId = ? AND msgs.msgId BETWEEN ? AND ?
			GROUP BY msgs.mboxId, msgs.msgId, seqnum`)
	} else {
		b.getMsgsNoBodyUid, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, NULL, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
				FROM msgs
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
			WHERE msgs.mboxId = ? AND msgs.msgId BETWEEN ? AND ?
			GROUP BY msgs.mboxId, msgs.msgId, seqnum`)
	}
	if err != nil {
		return errors.Wrap(err, "getMsgsNoBodyUid prep")
	}
	if b.db.mysql57 {
		b.getMsgsBodyUid, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, body, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT (@rownum := @rownum + 1) AS seqnum, msgId, mboxId
				FROM msgs, (SELECT @rownum := 0) counter
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
			WHERE msgs.mboxId = ? AND msgs.msgId BETWEEN ? AND ?
			GROUP BY seqnum, msgs.mboxId, msgs.msgId`)
	} else {
		b.getMsgsBodyUid, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, body, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
				FROM msgs
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
			WHERE msgs.mboxId = ? AND msgs.msgId BETWEEN ? AND ?
			GROUP BY seqnum, msgs.mboxId, msgs.msgId`)
	}
	if err != nil {
		return errors.Wrap(err, "getMsgsBodyUid prep")
	}
	if b.db.mysql57 {
		b.getMsgsNoBodySeq, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, NULL, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT (@rownum := @rownum + 1) AS seqnum, msgId, mboxId
				FROM msgs, (SELECT @rownum := 0) counter
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
			WHERE msgs.mboxId = ? AND seqnum BETWEEN ? AND ?
			GROUP BY seqnum, msgs.mboxId, msgs.msgId`)
	} else {
		b.getMsgsNoBodySeq, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, NULL, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
				FROM msgs
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
			WHERE msgs.mboxId = ? AND seqnum BETWEEN ? AND ?
			GROUP BY seqnum, msgs.mboxId, msgs.msgId`)
	}
	if err != nil {
		return errors.Wrap(err, "getMsgsNoBodySeq prep")
	}
	if b.db.mysql57 {
		b.getMsgsBodySeq, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, body, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT (@rownum := @rownum + 1) AS seqnum, msgId, mboxId
				FROM msgs, (SELECT @rownum := 0) counter
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId
			WHERE msgs.mboxId = ? AND seqnum BETWEEN ? AND ? AND msgs.mboxId = map.mboxId
			GROUP BY seqnum, msgs.mboxId, msgs.msgId`)
	} else {
		b.getMsgsBodySeq, err = b.db.Prepare(`
			SELECT seqnum, msgs.msgId, date, bodyLen, body, coalesce(` + b.groupConcatFn("flag", "{") + `, '')
			FROM msgs
			INNER JOIN (
				SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
				FROM msgs
				WHERE mboxId = ?
			) map
			ON map.msgId = msgs.msgId
			LEFT JOIN flags
			ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId
			WHERE msgs.mboxId = ? AND seqnum BETWEEN ? AND ? AND msgs.mboxId = map.mboxId
			GROUP BY seqnum, msgs.mboxId, msgs.msgId`)
	}
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
	if b.db.mysql57 {
		b.massClearFlagsSeq, err = b.db.Prepare(`
			DELETE FROM flags
			WHERE mboxId = ?
			AND msgId IN (
				SELECT msgId
				FROM (
					SELECT (@rownum := @rownum + 1) AS seqnum, msgId
					FROM msgs, (SELECT @rownum := 0) counter
					WHERE mboxId = ?
				) seq
				WHERE seqnum BETWEEN ? AND ?
			)`)
	} else {
		b.massClearFlagsSeq, err = b.db.Prepare(`
			DELETE FROM flags
			WHERE mboxId = ?
			AND msgId IN (
				SELECT msgId
				FROM (
					SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId
					FROM msgs
					WHERE mboxId = ?
				) seq
				WHERE seqnum BETWEEN ? AND ?
			)`)
	}
	if err != nil {
		return errors.Wrap(err, "massClearFlagsSeq prep")
	}

	return nil
}

func (b *Backend) groupConcatFn(expr, separator string) string {
	if b.driver == "sqlite3" {
		return "group_concat(" + expr + ", '" + separator + "')"
	}
	if b.driver == "postgres" {
		return "string_agg(" + expr + ", '" + separator + "')"
	}
	if b.driver == "mysql" {
		return "group_concat(" + expr + " SEPARATOR '" + separator + "')"
	}
	panic("Unsupported driver")
}

func (b *Backend) UserCreds(username string) (uint64, []byte, []byte, error) {
	row := b.userCreds.QueryRow(username)
	id, passHashHex, passSaltHex := uint64(0), "", ""
	if err := row.Scan(&id, &passHashHex, &passSaltHex); err != nil {
		return 0, nil, nil, err
	}

	passHash, err := hex.DecodeString(passHashHex)
	if err != nil {
		return 0, nil, nil, err
	}
	passSalt, err := hex.DecodeString(passSaltHex)
	if err != nil {
		return 0, nil, nil, err
	}

	return id, passHash, passSalt, nil
}

func (b *Backend) CreateUser(username, password string) error {
	salt := make([]byte, 16)
	if n, err := rand.Read(salt); err != nil {
		return errors.Wrap(err, "CreateUser")
	} else if n != 16 {
		return errors.New("CreateUser: failed to read enough entropy for salt from CSPRNG")
	}

	pass := make([]byte, 0, len(password)+len(salt))
	pass = append(pass, []byte(password)...)
	pass = append(pass, salt...)
	digest := sha3.Sum512(pass)

	_, err := b.addUser.Exec(username, hex.EncodeToString(digest[:]), hex.EncodeToString(salt))
	if err != nil && (strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "unique")) {
		return sqlmail.ErrUserAlreadyExists
	}
	return errors.Wrap(err, "CreateUser")
}

func (b *Backend) DeleteUser(username string) error {
	stats, err := b.delUser.Exec(username)
	if err != nil {
		return errors.Wrap(err, "DeleteUser")
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "SetUserPassword")
	}
	if affected == 0 {
		return sqlmail.ErrUserDoesntExists
	}
	return nil
}

func (b *Backend) SetUserPassword(username, newPassword string) error {
	salt := make([]byte, 16)
	if n, err := rand.Read(salt); err != nil {
		return errors.Wrap(err, "SetUserPassword")
	} else if n != 16 {
		return errors.New("SetUserPassword: failed to read enough entropy for salt from CSPRNG")
	}

	pass := make([]byte, 0, len(newPassword)+len(salt))
	pass = append(pass, []byte(newPassword)...)
	pass = append(pass, salt...)
	digest := sha3.Sum512(pass)

	stats, err := b.setUserPass.Exec(hex.EncodeToString(digest[:]), hex.EncodeToString(salt), username)
	affected, err := stats.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "SetUserPassword")
	}
	if affected == 0 {
		return sqlmail.ErrUserDoesntExists
	}
	return nil
}

func (b *Backend) GetUser(username string) (backend.User, error) {
	uid, _, _, err := b.UserCreds(username)
	if err != nil {
		return nil, sqlmail.ErrUserDoesntExists
	}
	return &User{id: uid, username: username, parent: b}, nil
}

func (b *Backend) Login(username, password string) (backend.User, error) {
	uid, passHash, passSalt, err := b.UserCreds(username)
	if err != nil {
		return nil, backend.ErrInvalidCredentials
	}

	pass := make([]byte, 0, len(password)+len(passSalt))
	pass = append(pass, []byte(password)...)
	pass = append(pass, passSalt...)
	digest := sha3.Sum512(pass)
	if !bytes.Equal(digest[:], passHash) {
		return nil, backend.ErrInvalidCredentials
	}

	return &User{id: uid, username: username, parent: b}, nil
}
