package imapsql

import (
	"database/sql"

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

func (b *Backend) upgradeSchema(currentVer int) error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Functions for schema upgrade go here. Example:
	//if currentVer == 1 {
	//	if err := b.schemaUpgrade1To2(tx); err != nil {
	//		return errors.Wrap(err, "1->2 upgrade")
	//	}
	//	currentVer = 2
	//}
	if currentVer == 2 {
		if err := b.schemaUpgrade2To3(tx); err != nil {
			return errors.Wrap(err, "2->3 upgrade")
		}
	}

	if currentVer != SchemaVersion {
		return errors.New("database schema version is too old and can't be upgraded using this go-imap-sql version")
	}
	return tx.Commit()
}

func (b *Backend) schemaUpgrade2To3(tx *sql.Tx) error {
	_, err := tx.Exec(b.db.rewriteSQL(`UPDATE users SET password = 'sha3-512:' || password`))
	return err
}
