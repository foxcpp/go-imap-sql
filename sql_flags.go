package imapsql

import (
	"database/sql"

	imap "github.com/emersion/go-imap"
)

func (b *Backend) buildFlagsAddStmt(uid bool, flags []string) string {
	if uid {
		return `
			INSERT INTO flags
			SELECT ? AS mboxId, msgId, column1 AS flag
			FROM msgs
			CROSS JOIN (` + b.db.valuesSubquery(flags) + `) flagset
			WHERE mboxId = ? AND msgId BETWEEN ? AND ?
			ON CONFLICT DO NOTHING`
	}

	// ON 1=1 is necessary to make SQLite's parser not interpret ON CONFLICT as join condition.
	if b.db.driver == "sqlite3" {
		return `
            INSERT INTO flags
            SELECT ? AS mboxId, msgId, column1 AS flag
            FROM (SELECT msgId FROM msgs WHERE mboxId = ? LIMIT ? OFFSET ?) msgIds
            CROSS JOIN (` + b.db.valuesSubquery(flags) + `) flagset ON 1=1
            ON CONFLICT DO NOTHING`
	} else {
		// But 1 = 1 in query causes errors on PostgreSQL.
		return `
            INSERT INTO flags
            SELECT ? AS mboxId, msgId, column1 AS flag
            FROM (SELECT msgId FROM msgs WHERE mboxId = ? LIMIT ? OFFSET ?) msgIds
            CROSS JOIN (` + b.db.valuesSubquery(flags) + `) flagset
            ON CONFLICT DO NOTHING`
	}
}

func (m *Mailbox) makeFlagsAddStmtArgs(uid bool, flags []string, seq imap.Seq) (params []interface{}) {
	start, stop := sqlRange(seq)
	if uid {
		params = make([]interface{}, 0, 4+len(flags))
		params = append(params, m.id)
	} else {
		params = make([]interface{}, 0, 4+len(flags))
		params = append(params, m.id, m.id, stop-start+1, start-1)
	}
	for _, flag := range flags {
		params = append(params, flag)
	}

	if uid {
		params = append(params, m.id, start, stop)
	}
	return
}

func (b *Backend) getFlagsAddStmt(uid bool, flags []string) (*sql.Stmt, error) {
	str := b.buildFlagsAddStmt(uid, flags)
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

func (b *Backend) buildFlagsRemStmt(uid bool, flags []string) string {
	if uid {
		return `
			 DELETE FROM flags
			 WHERE mboxId = ?
			 AND msgId BETWEEN ? AND ?
			 AND flag IN (` + b.db.valuesSubquery(flags) + `)`
	}
	return `
         DELETE FROM flags
         WHERE mboxId = ?
         AND msgId IN (
                 SELECT msgId
                 FROM (
                         SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId
                         FROM msgs
                         WHERE mboxId = ?
                 ) seqnums
                 WHERE seqnum BETWEEN ? AND ?
         ) AND flag IN (` + b.db.valuesSubquery(flags) + `)`
}

func (b *Backend) getFlagsRemStmt(uid bool, flags []string) (*sql.Stmt, error) {
	str := b.buildFlagsRemStmt(uid, flags)
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

func (m *Mailbox) makeFlagsRemStmtArgs(uid bool, flags []string, seq imap.Seq) (params []interface{}) {
	start, stop := sqlRange(seq)
	if uid {
		params = make([]interface{}, 0, 3+len(flags))
		params = append(params, m.id, start, stop)
	} else {
		params = make([]interface{}, 0, 4+len(flags))
		params = append(params, m.id, m.id, start, stop)
	}
	for _, flag := range flags {
		params = append(params, flag)
	}
	return
}
