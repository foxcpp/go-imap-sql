package imapsql

import (
	"database/sql"
	"fmt"
	"strconv"
)

func buildSearchStmt(uid bool, withFlags, withoutFlags []string) string {
	var stmt string
	if uid {
		stmt += `
			SELECT DISTINCT msgId
			FROM flags
			WHERE mboxId = ?
			`
	} else {
		stmt += `
			SELECT DISTINCT seqnum
			FROM flags
			INNER JOIN (
				SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId
				FROM msgs
				WHERE mboxId = ?
			) map
			ON map.msgId = flags.msgId
			WHERE mboxId = ?
			`
	}

	if len(withFlags) != 0 {
		if len(withFlags) == 1 {
			stmt += `AND flag = ? `
		} else {
			stmt += `AND flag IN (`
			for i := range withFlags {
				stmt += `?`
				if i != len(withFlags)-1 {
					stmt += `, `
				}
			}
			stmt += `)`
		}
	}
	if len(withoutFlags) != 0 {
		stmt += `AND flags.msgId NOT IN (` + buildSearchStmt(true, withoutFlags, nil) + `)`
	}
	if len(withFlags) > 1 {
		stmt += `GROUP BY msgId HAVING COUNT() = ` + strconv.Itoa(len(withFlags))
	}

	return stmt
}

func (m *Mailbox) getFlagSearchStmt(uid bool, withFlags, withoutFlags []string) (*sql.Stmt, error) {
	cacheKey := fmt.Sprint(uid, len(withFlags), ":", len(withoutFlags))
	m.parent.flagsSearchStmtsLck.RLock()
	stmt := m.parent.flagsSearchStmtsCache[cacheKey]
	m.parent.flagsSearchStmtsLck.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	stmtStr := buildSearchStmt(uid, withFlags, withoutFlags)
	stmt, err := m.parent.db.Prepare(stmtStr)
	if err != nil {
		return nil, err
	}
	if len(withFlags) < 3 && len(withoutFlags) < 3 {
		m.parent.flagsSearchStmtsLck.Lock()
		m.parent.flagsSearchStmtsCache[cacheKey] = stmt
		m.parent.flagsSearchStmtsLck.Unlock()
	}

	return stmt, nil
}

func (m *Mailbox) buildFlagSearchQueryArgs(uid bool, withFlags, withoutFlags []string) []interface{} {
	queryArgs := make([]interface{}, 0, 2+len(withFlags)+1+len(withoutFlags))
	queryArgs = append(queryArgs, m.id)
	if !uid {
		queryArgs = append(queryArgs, m.id)
	}
	for _, flag := range withFlags {
		queryArgs = append(queryArgs, flag)
	}
	if len(withoutFlags) != 0 {
		queryArgs = append(queryArgs, m.id)
		for _, flag := range withoutFlags {
			queryArgs = append(queryArgs, flag)
		}
	}
	return queryArgs
}
