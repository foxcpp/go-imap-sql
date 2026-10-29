package imapsql

import (
	"database/sql"
	"fmt"
	"strconv"
)

func buildSearchStmt(withFlags, withoutFlags []string, group bool) string {
	var stmt string
	stmt += `
		SELECT DISTINCT msgs.msgId
		FROM msgs
		LEFT JOIN flags ON msgs.msgId = flags.msgid AND msgs.mboxId = flags.mboxId
		WHERE msgs.mboxId = ?
		`

	if len(withFlags) != 0 {
		if len(withFlags) == 1 {
			stmt += `AND flags.flag = ? `
		} else {
			stmt += `AND flags.flag IN (`
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
		// The nested query identifies messages having any excluded flag. It
		// must not require all flags with GROUP BY/HAVING, unlike the
		// top-level query when multiple flags are required.
		stmt += `AND msgs.msgId NOT IN (` + buildSearchStmt(withoutFlags, nil, false) + `)`
	}
	if group && len(withFlags) > 1 {
		stmt += `GROUP BY flags.msgId HAVING COUNT(*) = ` + strconv.Itoa(len(withFlags))
	}

	return stmt
}

func (m *Mailbox) getFlagSearchStmt(withFlags, withoutFlags []string) (*sql.Stmt, error) {
	cacheKey := fmt.Sprint(len(withFlags), ":", len(withoutFlags))
	m.parent.flagsSearchStmtsLck.RLock()
	stmt := m.parent.flagsSearchStmtsCache[cacheKey]
	m.parent.flagsSearchStmtsLck.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	stmtStr := buildSearchStmt(withFlags, withoutFlags, true)
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

func (m *Mailbox) buildFlagSearchQueryArgs(withFlags, withoutFlags []string) []interface{} {
	queryArgs := make([]interface{}, 0, 2+len(withFlags)+1+len(withoutFlags))
	queryArgs = append(queryArgs, m.id)
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
