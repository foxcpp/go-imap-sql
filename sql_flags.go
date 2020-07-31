package imapsql

import (
	"database/sql"
)

func (b *Backend) buildFlagsAddStmt(flagsCount int) string {
	return `
		INSERT INTO flags
		SELECT mboxId, msgId, column1 AS flag
		FROM msgs
		CROSS JOIN (` + b.db.valuesSubquery(flagsCount) + `) flagset
		WHERE mboxId = ? AND msgId BETWEEN ? AND ?
		ON CONFLICT DO NOTHING`
}

func (m *Mailbox) makeFlagsAddStmtArgs(flags []string, start, stop uint32) (params []interface{}) {
	params = make([]interface{}, 0, 3+len(flags))
	for _, flag := range flags {
		params = append(params, flag)
	}

	params = append(params, m.id, start, stop)
	return
}

func (b *Backend) getFlagsAddStmt(flagsCount int) (*sql.Stmt, error) {
	str := b.buildFlagsAddStmt(flagsCount)
	b.addFlagsStmtsLck.RLock()
	stmt := b.addFlagsStmtsCache[str]
	b.addFlagsStmtsLck.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	stmt, err := b.db.Prepare(str)
	if err != nil {
		return nil, err
	}

	b.addFlagsStmtsLck.Lock()
	b.addFlagsStmtsCache[str] = stmt
	b.addFlagsStmtsLck.Unlock()
	return stmt, nil
}

func (b *Backend) buildFlagsRemStmt(flagsCount int) string {
	return `
		DELETE FROM flags
		WHERE mboxId = ?
		AND msgId BETWEEN ? AND ?
		AND flag IN (` + b.db.valuesSubquery(flagsCount) + `)`
}

func (b *Backend) getFlagsRemStmt(flagsCount int) (*sql.Stmt, error) {
	str := b.buildFlagsRemStmt(flagsCount)
	b.remFlagsStmtsLck.RLock()
	stmt := b.remFlagsStmtsCache[str]
	b.remFlagsStmtsLck.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	stmt, err := b.db.Prepare(str)
	if err != nil {
		return nil, err
	}

	b.remFlagsStmtsLck.Lock()
	b.remFlagsStmtsCache[str] = stmt
	b.remFlagsStmtsLck.Unlock()
	return stmt, nil
}

func (m *Mailbox) makeFlagsRemStmtArgs(flags []string, start, stop uint32) []interface{} {
	params := make([]interface{}, 0, 3+len(flags))
	params = append(params, m.id, start, stop)
	for _, flag := range flags {
		params = append(params, flag)
	}
	return params
}
