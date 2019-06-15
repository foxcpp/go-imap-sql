package imapsql

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func (b *Backend) addSqlite3Params(dsn string) string {
	if !strings.HasPrefix(dsn, "file:") {
		dsn = "file:" + dsn
	}
	if !strings.Contains(dsn, "?") {
		dsn += "?"
	} else {
		dsn += "&"
	}

	dsn += "_fk=ON&_auto_vacuum=FULL&"

	if !b.Opts.NoWAL {
		dsn += "_journal_mode=WAL&_sync=NORMAL&"
	}
	if b.Opts.ExclusiveLock {
		dsn += "_locking_mode=EXCLUSIVE&"
	}

	if b.Opts.BusyTimeout == 0 {
		b.Opts.BusyTimeout = 500000
	}
	if b.Opts.BusyTimeout == -1 {
		b.Opts.BusyTimeout = 0
	}
	dsn += "_busy_timeout=" + strconv.Itoa(b.Opts.BusyTimeout)

	return dsn
}

func (b *Backend) configureEngine() error {
	if b.db.driver == "sqlite3" {
		// For testing purposes, it is important that only one memory DB will
		// be used (otherwise each connection will get its own DB)
		if b.db.dsn == ":memory:" {
			b.db.DB.SetMaxOpenConns(1)
		}

		if b.Opts.ExternalStore == nil {
			if _, err := b.db.Exec(`PRAGMA page_size=16384`); err != nil {
				return err
			}
		}
	}

	if b.db.driver == "mysql" {
		// Make MySQL more ANSI SQL compatible.
		_, err := b.db.Exec(`SET SESSION sql_mode = 'ansi,no_backslash_escapes'`)
		if err != nil {
			return err
		}

		// Turn on strictiest transaction isolation.
		// TODO: Review if this is really needed to ensure consistentcy.
		_, err = b.db.Exec(`SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE`)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) initSchema() error {
	var err error
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id BIGSERIAL NOT NULL PRIMARY KEY AUTOINCREMENT,
			username VARCHAR(255) NOT NULL UNIQUE,
			msgsizelimit INTEGER DEFAULT NULL,
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
			msgsizelimit INTEGER DEFAULT NULL,
			uidnext INTEGER NOT NULL DEFAULT 1,
			uidvalidity BIGINT NOT NULL,
            specialuse VARCHAR(255) DEFAULT NULL,

			UNIQUE(uid, name)
		)`)
	if err != nil {
		return errors.Wrap(err, "create table mboxes")
	}
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS extKeys (
			id VARCHAR(255) PRIMARY KEY NOT NULL,
			refs INTEGER NOT NULL DEFAULT 1
		)`)
	if err != nil {
		return errors.Wrap(err, "create table extkeys")
	}
	_, err = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS msgs (
			mboxId BIGINT NOT NULL REFERENCES mboxes(id) ON DELETE CASCADE,
			msgId BIGINT NOT NULL,
			date BIGINT NOT NULL,
			bodyLen INTEGER NOT NULL,
			body LONGTEXT,
			mark INTEGER NOT NULL DEFAULT 0,

			header LONGTEXT,
			bodyStructure LONGTEXT NOT NULL,
			cachedHeader LONGTEXT NOT NULL,
			extBodyKey VARCHAR(255) DEFAULT NULL REFERENCES extKeys(id) ON DELETE RESTRICT,

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
			UNIQUE (mboxId, msgId, flag)
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
		return errors.Wrap(err, "userCreds prep")
	}
	b.listUsers, err = b.db.Prepare(`
		SELECT id, username
		FROM users`)
	if err != nil {
		return errors.Wrap(err, "listUsers prep")
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
		INSERT INTO mboxes(uid, name, uidvalidity, specialuse)
		VALUES (?, ?, ?, ?)`)
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
	b.getMboxAttrs, err = b.db.Prepare(`
		SELECT mark, specialuse FROM mboxes
		WHERE uid = ? AND name = ?`)
	if err != nil {
		return errors.Wrap(err, "getMboxAttrs prep")
	}
	b.setSubbed, err = b.db.Prepare(`
		UPDATE mboxes SET sub = ?
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "setSubbed prep")
	}
	b.hasChildren, err = b.db.Prepare(`
		SELECT count(*)
		FROM mboxes
		WHERE name LIKE ? AND uid = ?`)
	if err != nil {
		return errors.Wrap(err, "hasChildren prep")
	}
	b.uidNext, err = b.db.Prepare(`
		SELECT uidnext
		FROM mboxes
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "uidNext prep")
	}
	b.addUidNext, err = b.db.Prepare(`
		UPDATE mboxes
		SET uidnext = uidnext + ?
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "addUidNext prep")
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
	if err != nil {
		return errors.Wrap(err, "firstUnseenSeqNum prep")
	}
	b.deletedSeqnums, err = b.db.Prepare(`
		SELECT seqnum
		FROM (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId
			FROM msgs
			WHERE mboxId = ?
		) seqnums
		WHERE msgId IN (
			SELECT msgId
			FROM flags
			WHERE mboxId = ?
			AND flag = '\Deleted'
		)
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "deletedSeqnums prep")
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
		INSERT INTO msgs(mboxId, msgId, date, bodyLen, body, header, bodyStructure, cachedHeader, extBodyKey)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return errors.Wrap(err, "addMsg prep")
	}
	b.copyMsgsUid, err = b.db.Prepare(`
		INSERT INTO msgs
		SELECT ? AS mboxId, (
			SELECT uidnext - 1
			FROM mboxes
			WHERE id = ?
		) + row_number() OVER (ORDER BY msgId), date, bodyLen, body, 0 AS mark, header, bodyStructure, cachedHeader, extBodyKey
		FROM msgs
		WHERE mboxId = ? AND msgId BETWEEN ? AND ?`)
	if err != nil {
		return errors.Wrap(err, "copyMsgsUid prep")
	}
	b.copyMsgFlagsUid, err = b.db.Prepare(`
		INSERT INTO flags
		SELECT ?, new_msgId AS msgId, flag
		FROM flags
		INNER JOIN (
			SELECT (
				SELECT uidnext - 1
				FROM mboxes
				WHERE id = ?
			) + row_number() OVER (ORDER BY msgId) AS new_msgId, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
			AND msgId BETWEEN ? AND ?
		) map ON map.msgId = flags.msgId
		AND map.mboxId = flags.mboxId`)
	if err != nil {
		return errors.Wrap(err, "copyMsgFlagsUid prep")
	}
	b.copyMsgsSeq, err = b.db.Prepare(`
		INSERT INTO msgs
		SELECT ? AS mboxId, (
			SELECT uidnext - 1
			FROM mboxes
			WHERE id = ?
		) + row_number() OVER (ORDER BY msgId), date, bodyLen, body, 0 AS mark, header, bodyStructure, cachedHeader, extBodyKey
		FROM (
			SELECT msgId, date, bodyLen, body,  header, bodyStructure, cachedHeader, extBodyKey
			FROM msgs
			WHERE mboxId = ?
			ORDER BY msgId
			LIMIT ? OFFSET ?
		) subset`)
	if err != nil {
		return errors.Wrap(err, "copyMsgsSeq prep")
	}
	b.copyMsgFlagsSeq, err = b.db.Prepare(`
		INSERT INTO flags
		SELECT ?, new_msgId AS msgId, flag
		FROM flags
		INNER JOIN (
			SELECT (
				SELECT uidnext - 1
				FROM mboxes
				WHERE id = ?
			) + row_number() OVER (ORDER BY msgId) AS new_msgId, msgId, mboxId
			FROM (
				SELECT msgId, mboxId
				FROM msgs
				WHERE mboxId = ?
				ORDER BY msgId
				LIMIT ? OFFSET ?
			) subset
		) map ON map.msgId = flags.msgId
		AND map.mboxId = flags.mboxId`)
	if err != nil {
		return errors.Wrap(err, "copyMsgFlagsSeq prep")
	}
	b.massClearFlagsUid, err = b.db.Prepare(`
		DELETE FROM flags
		WHERE mboxId = ?
		AND msgId BETWEEN ? AND ?
		AND flag != '\Recent'`)
	if err != nil {
		return errors.Wrap(err, "massClearFlagsUid prep")
	}
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
		)
		AND flag != '\Recent'`)
	if err != nil {
		return errors.Wrap(err, "massClearFlagsSeq prep")
	}

	b.addRecentToLast, err = b.db.Prepare(`
		INSERT INTO flags
		SELECT ? AS mboxId, msgId, '\Recent'
		FROM (SELECT msgId FROM msgs WHERE mboxId = ? ORDER BY msgId DESC LIMIT ?) targets
		ON CONFLICT DO NOTHING
		`)
	if err != nil {
		return errors.Wrap(err, "addRecenttoLast prep")
	}

	b.markUid, err = b.db.Prepare(`
		UPDATE msgs
		SET mark = 1
		WHERE mboxId = ?
		AND msgId BETWEEN ? AND ?`)
	if err != nil {
		return errors.Wrap(err, "delMsgsUid prep")
	}
	b.markSeq, err = b.db.Prepare(`
		UPDATE msgs
		SET mark = 1
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
	if err != nil {
		return errors.Wrap(err, "delMsgsSeq prep")
	}
	b.delMarked, err = b.db.Prepare(`
		DELETE FROM msgs
		WHERE mark = 1`)
	if err != nil {
		return errors.Wrap(err, "delMarked prep")
	}
	b.markedSeqnums, err = b.db.Prepare(`
		SELECT seqnum, extBodyKey
		FROM (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, mark, extBodyKey
			FROM msgs
			WHERE mboxId = ?
		) seqnums
		WHERE mark = 1
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "markedSeqnums prep")
	}

	b.setUserMsgSizeLimit, err = b.db.Prepare(`
		UPDATE users
		SET msgsizelimit = ?
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "setUserMsgSizeLimit prep")
	}
	b.userMsgSizeLimit, err = b.db.Prepare(`
		SELECT msgsizelimit
		FROM users
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "userMsgSizeLimit prep")
	}
	b.setMboxMsgSizeLimit, err = b.db.Prepare(`
		UPDATE mboxes
		SET msgsizelimit = ?
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "setUserMsgSizeLimit prep")
	}
	b.mboxMsgSizeLimit, err = b.db.Prepare(`
		SELECT msgsizelimit
		FROM mboxes
		WHERE id = ?`)
	if err != nil {
		return errors.Wrap(err, "userMsgSizeLimit prep")
	}

	b.msgFlagsUid, err = b.db.Prepare(`
		SELECT seqnum, msgs.msgId, ` + b.db.aggrValuesSet("flag", "{") + `
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
		GROUP BY msgs.mboxId, msgs.msgId, seqnum
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "msgFlagsUid prep")
	}
	b.msgFlagsSeq, err = b.db.Prepare(`
		SELECT seqnum, msgs.msgId, ` + b.db.aggrValuesSet("flag", "{") + `
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
		GROUP BY msgs.mboxId, msgs.msgId, seqnum
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "msgFlagsSeq prep")
	}

	b.usedFlags, err = b.db.Prepare(`
		SELECT DISTINCT flag
		FROM flags
		WHERE mboxId = ?`)
	if err != nil {
		return errors.Wrap(err, "usedFlags prep")
	}
	b.listMsgUids, err = b.db.Prepare(`
        SELECT msgId
        FROM msgs
        WHERE mboxId = ?`)
	if err != nil {
		return errors.Wrap(err, "listMsgUids prep")
	}

	b.searchFetchNoBody, err = b.db.Prepare(`
		SELECT seqnum, msgs.msgId, date, bodyLen, ` + b.db.aggrValuesSet("flag", "{") + `
		FROM msgs
		INNER JOIN (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
		) map
		ON map.msgId = msgs.msgId
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
		WHERE msgs.mboxId = ?
		GROUP BY msgs.mboxId, msgs.msgId, seqnum
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "searchFetchNoBody prep")
	}
	b.searchFetch, err = b.db.Prepare(`
		SELECT seqnum, msgs.msgId, date, header, bodyLen, extBodyKey, body, ` + b.db.aggrValuesSet("flag", "{") + `
		FROM msgs
		INNER JOIN (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
		) map
		ON map.msgId = msgs.msgId
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND flags.mboxId = map.mboxId AND msgs.mboxId = flags.mboxId
		WHERE msgs.mboxId = ?
		GROUP BY msgs.mboxId, msgs.msgId, seqnum
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "searchFetch prep")
	}

	// It is kinda expensive to compute sequence numbers using row_number() so we avoid it where possible.
	b.searchFetchNoBodyNoSeq, err = b.db.Prepare(`
		SELECT 0 AS seqnum, msgs.msgId, date, bodyLen, ` + b.db.aggrValuesSet("flag", "{") + `
		FROM msgs
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND msgs.mboxId = flags.mboxId
		WHERE msgs.mboxId = ?
		GROUP BY msgs.mboxId, msgs.msgId, seqnum
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "searchFetchNoBodyNoSeq prep")
	}
	b.searchFetchNoSeq, err = b.db.Prepare(`
		SELECT 0 AS seqnum, msgs.msgId, date, header, bodyLen, extBodyKey, body, ` + b.db.aggrValuesSet("flag", "{") + `
		FROM msgs
		LEFT JOIN flags
		ON flags.msgId = msgs.msgId AND msgs.mboxId = flags.mboxId
		WHERE msgs.mboxId = ?
		GROUP BY msgs.mboxId, msgs.msgId, seqnum
		ORDER BY seqnum DESC`)
	if err != nil {
		return errors.Wrap(err, "searchFetchNoSeq prep")
	}

	b.addExtKey, err = b.db.Prepare(`
		INSERT INTO extKeys(id)
		VALUES (?)`)
	if err != nil {
		return errors.Wrap(err, "addExtKey prep")
	}
	b.decreaseRefForMarked, err = b.db.Prepare(`
		UPDATE extKeys
		SET refs = refs - 1
		WHERE id IN (
			SELECT extBodyKey
			FROM msgs
			WHERE mboxId = ? AND mark = 1 AND extBodyKey IS NOT NULL
		)`)
	if err != nil {
		return errors.Wrap(err, "decreaseRefForMarked prep")
	}
	b.decreaseRefForDeleted, err = b.db.Prepare(`
		UPDATE extKeys
		SET refs = refs - 1
		WHERE id IN (
			SELECT extBodyKey
			FROM msgs
			INNER JOIN flags
			ON msgs.mboxId = flags.mboxId
			AND msgs.msgId = msgs.msgId
			AND flag = '\Deleted'
			WHERE msgs.mboxId = ?
		)`)
	if err != nil {
		return errors.Wrap(err, "decreaseRefForDeleted prep")
	}
	b.incrementRefUid, err = b.db.Prepare(`
		UPDATE extKeys
		SET refs = refs + 1
		WHERE id IN (
			SELECT extBodyKey
			FROM msgs
			WHERE mboxId = ? AND msgId BETWEEN ? AND ?
			ORDER BY msgId DESC
		)`)
	if err != nil {
		return errors.Wrap(err, "incrementRefUid prep")
	}
	b.incrementRefSeq, err = b.db.Prepare(`
		UPDATE extKeys
		SET refs = refs + 1
		WHERE id IN (
			SELECT extBodyKey
			FROM msgs
			INNER JOIN (
				SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId
				FROM msgs
				WHERE mboxId = ?
			) map
			ON msgs.msgId = map.msgId
			WHERE mboxId = ? AND seqnum BETWEEN ? AND ?
			ORDER BY msgs.msgId DESC
		)`)
	if err != nil {
		return errors.Wrap(err, "incrementRefSeq prep")
	}
	b.zeroRef, err = b.db.Prepare(`
		SELECT extBodyKey
		FROM msgs
		INNER JOIN extKeys
		ON msgs.extBodyKey = extKeys.id
		WHERE extBodyKey IS NOT NULL
		AND mboxId = ?
		AND refs = 0`)
	if err != nil {
		return errors.Wrap(err, "zeroRef prep")
	}
	b.deleteZeroRef, err = b.db.Prepare(`
		DELETE FROM extKeys
		WHERE refs = 0`)
	if err != nil {
		return errors.Wrap(err, "deleteZeroRef prep")
	}

	return nil
}
