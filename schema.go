package imapsql

import (
	"bytes"
	"database/sql"
	"log"

	"github.com/pkg/errors"
)

func (b *Backend) schemaVersion() (int, error) {
	_, err := b.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version ( version INTEGER NOT NULL )`)
	if err != nil {
		return 0, err
	}

	row := b.db.QueryRow(`SELECT version FROM schema_version`)
	var version int
	if err := row.Scan(&version); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return version, nil
}

func (b *Backend) setSchemaVersion(newVer int) error {
	_, err := b.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version ( version INTEGER NOT NULL )`)
	if err != nil {
		return err
	}

	_, err = b.db.Exec(`UPDATE schema_version SET version = ?`, newVer)
	return err
}

func (b *Backend) upgradeSchema(previousVer int) error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if previousVer < 2 {
		if err := b.schemaUpgradeTo2(tx); err != nil {
			return errors.Wrap(err, "1->2 upgrade")
		}
	}

	return tx.Commit()
}

func (b *Backend) schemaUpgradeTo2(tx *sql.Tx) error {
	rows, err := tx.Query(b.db.rewriteSQL(`SELECT mboxId, msgId, body FROM msgs`))
	if err != nil {
		return err
	}

	if _, err := tx.Exec(b.db.rewriteSQL(`ALTER TABLE msgs ADD COLUMN header LONGTEXT`)); err != nil {
		return errors.Wrap(err, "add col header")
	}
	// TODO: Figure out how to add NOT NULL constrait.
	if _, err := tx.Exec(b.db.rewriteSQL(`ALTER TABLE msgs ADD COLUMN bodyStructure LONGTEXT`)); err != nil {
		return errors.Wrap(err, "add col bodyStructure")
	}
	if _, err := tx.Exec(b.db.rewriteSQL(`ALTER TABLE msgs ADD COLUMN cachedHeader LONGTEXT`)); err != nil {
		return errors.Wrap(err, "add col cachedHeader")
	}
	if _, err := tx.Exec(b.db.rewriteSQL(`ALTER TABLE msgs ADD COLUMN extBodyKey VARCHAR(255) REFERENCES extKeys(id) ON DELETE RESTRICT DEFAULT NULL`)); err != nil {
		return errors.Wrap(err, "add col extBodyKey")
	}

	updStmt, err := tx.Prepare(b.db.rewriteSQL(`
		UPDATE msgs
		SET body = ?,
			header = ?,
			bodyStructure = ?,
			cachedHeader = ?
		WHERE mboxId = ?
		AND msgId = ?`))
	if err != nil {
		return err
	}

	for rows.Next() {
		var mboxId, msgId uint
		var srcBody []byte

		if err := rows.Scan(&mboxId, &msgId, &srcBody); err != nil {
			return err
		}

		hdr, body := splitHeader(srcBody)
		bodyStruct, cachedHdr, err := extractCachedData(bytes.NewReader(srcBody))
		if err != nil {
			log.Printf("Possibly non RFC 282 message in database, it will remain in DB but will be unreadable, mboxId = %d, msgId = %d\n", mboxId, msgId)
			log.Printf("extractCachedData failed: %v\n", err)
		}

		if _, err := updStmt.Exec(body, hdr, bodyStruct, cachedHdr, mboxId, msgId); err != nil {
			return err
		}
	}

	return nil
}
