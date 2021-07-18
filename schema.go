package imapsql

import (
	"database/sql"
	"errors"
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

	info, err := b.db.Exec(`UPDATE schema_version SET version = ?`, newVer)
	if err != nil {
		return err
	}
	affected, err := info.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		_, err = b.db.Exec(`INSERT INTO schema_version VALUES (?)`, newVer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) upgradeSchema(currentVer int) error {
	tx, err := b.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Functions for schema upgrade go here. Example:
	//if currentVer == 1 {
	//	if err := b.schemaUpgrade1To2(tx); err != nil {
	//		return wrapErr(err, "1->2 upgrade")
	//	}
	//	currentVer = 2
	//}

	if currentVer == 5 {
		_, err = b.DB.Exec(`ALTER TABLE msgs ADD COLUMN recent INTEGER NOT NULL DEFAULT 1`)
		if err != nil {
			return wrapErr(err, "5->6 upgrade")
		}
	}

	if currentVer != SchemaVersion {
		return errors.New("database schema version is too old and can't be upgraded using this go-imap-sql version")
	}
	return tx.Commit()
}
