package imapsql

import (
	"bytes"
	"database/sql"
	"io"
	"strings"
	"time"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
)

func (m *Mailbox) SearchMessages(uid bool, criteria *imap.SearchCriteria) ([]uint32, error) {
	needBody := searchNeedsBody(criteria)
	var rows *sql.Rows
	var err error
	if needBody {
		rows, err = m.parent.searchFetch.Query(m.id, m.id)
	} else {
		rows, err = m.parent.searchFetchNoBody.Query(m.id, m.id)
	}
	defer rows.Close()

	var res []uint32
	for rows.Next() {
		var seqNum, msgId uint32
		var dateUnix int64
		var headerLen, bodyLen int
		var header, body []byte
		var flagStr string

		if needBody {
			err = rows.Scan(&seqNum, &msgId, &dateUnix, &headerLen, &header, &bodyLen, &body, &flagStr)
		} else {
			err = rows.Scan(&seqNum, &msgId, &dateUnix, &headerLen, &bodyLen, &flagStr)
		}
		if err != nil {
			return nil, err
		}

		flags := strings.Split(flagStr, flagsSep)
		if len(flags) == 1 && flags[0] == "" {
			flags = nil
		}

		if !backendutil.MatchSeqNumAndUid(seqNum, msgId, criteria) {
			continue
		}
		if !backendutil.MatchFlags(flags, criteria) {
			continue
		}
		if !backendutil.MatchDate(time.Unix(dateUnix, 0), criteria) {
			continue
		}

		if needBody {
			ent, err := message.Read(io.MultiReader(bytes.NewReader(header), bytes.NewReader(body)))
			if err != nil {
				return nil, err
			}

			matched, err := backendutil.Match(ent, criteria)
			if err != nil {
				return nil, err
			}

			if !matched {
				continue
			}
		}

		if uid {
			res = append(res, msgId)
		} else {
			res = append(res, seqNum)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func searchNeedsBody(criteria *imap.SearchCriteria) bool {
	if criteria.Header != nil ||
		criteria.Body != nil ||
		criteria.Text != nil ||
		!criteria.SentSince.IsZero() ||
		!criteria.SentBefore.IsZero() ||
		criteria.Smaller != 0 ||
		criteria.Larger != 0 {

		return true
	}

	for _, crit := range criteria.Not {
		if searchNeedsBody(crit) {
			return true
		}
	}
	for _, crit := range criteria.Or {
		if searchNeedsBody(crit[0]) || searchNeedsBody(crit[1]) {
			return true
		}
	}

	return false
}
