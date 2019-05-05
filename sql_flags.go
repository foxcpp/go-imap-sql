package imapsql

import (
	"database/sql"

	imap "github.com/emersion/go-imap"
)

func (m *Mailbox) makeFlagsAddStmt(uid bool, flags []string) (*sql.Stmt, error) {
	var query *sql.Stmt
	var err error
	if uid {
		query, err = m.parent.db.Prepare(`
			INSERT INTO flags
			SELECT ? AS mboxId, msgId, column1 AS flag
			FROM msgs
			CROSS JOIN (` + m.valuesSubquery(flags) + `) flagset
			WHERE mboxId = ? AND msgId BETWEEN ? AND ?
			ON CONFLICT DO NOTHING`)
	} else {
		// ON 1=1 is necessary to make SQLite's parser not interpret ON CONFLICT as join condition.
		if m.parent.db.driver == "sqlite3" {
			query, err = m.parent.db.Prepare(`
				INSERT INTO flags
				SELECT ? AS mboxId, msgId, column1 AS flag
				FROM (SELECT msgId FROM msgs WHERE mboxId = ? LIMIT ? OFFSET ?) msgIds
				CROSS JOIN (` + m.valuesSubquery(flags) + `) flagset ON 1=1
				ON CONFLICT DO NOTHING`)
		} else {
			// But 1 = 1 in query causes errors on PostgreSQL.
			query, err = m.parent.db.Prepare(`
				INSERT INTO flags
				SELECT ? AS mboxId, msgId, column1 AS flag
				FROM (SELECT msgId FROM msgs WHERE mboxId = ? LIMIT ? OFFSET ?) msgIds
				CROSS JOIN (` + m.valuesSubquery(flags) + `) flagset
				ON CONFLICT DO NOTHING`)
		}
	}
	return query, err
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

func (m *Mailbox) makeFlagsRemStmt(uid bool, flags []string) (*sql.Stmt, error) {
	var query *sql.Stmt
	var err error
	if uid {
		query, err = m.parent.db.Prepare(`
			 DELETE FROM flags
			 WHERE mboxId = ?
			 AND msgId BETWEEN ? AND ?
			 AND flag IN (` + m.valuesSubquery(flags) + `)`)
	} else {
		query, err = m.parent.db.Prepare(`
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
			 ) AND flag IN (` + m.valuesSubquery(flags) + `)`)
	}
	return query, err
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
