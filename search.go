package imapsql

import (
	"bufio"
	"database/sql"
	"net/textproto"
	"strings"
	"time"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
)

func (m *Mailbox) SearchMessages(uid bool, criteria *imap.SearchCriteria) ([]uint32, error) {
	if searchOnlyWithFlags(criteria) {
		return m.flagSearch(uid, criteria.WithFlags, criteria.WithoutFlags)
	}

	needBody := searchNeedsBody(criteria)
	noSeqNum := noSeqNumNeeded(criteria)
	var rows *sql.Rows
	var err error
	if needBody {
		if noSeqNum && uid {
			rows, err = m.parent.searchFetchNoSeq.Query(m.id)
		} else {
			rows, err = m.parent.searchFetch.Query(m.id, m.id)
		}
	} else {
		if noSeqNum && uid {
			rows, err = m.parent.searchFetchNoBodyNoSeq.Query(m.id)
		} else {
			rows, err = m.parent.searchFetchNoBody.Query(m.id, m.id)
		}
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []uint32
	for rows.Next() {
		var seqNum, msgId uint32
		var dateUnix int64
		var headerLen, bodyLen int
		var headerBlob, bodyBlob []byte
		var flagStr, extBodyKey string

		if needBody {
			err = rows.Scan(&seqNum, &msgId, &dateUnix, &headerLen, &headerBlob, &bodyLen, &extBodyKey, &bodyBlob, &flagStr)
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
			body, err := m.openBody(extBodyKey, headerBlob, bodyBlob)
			if err != nil {
				return nil, err
			}
			bufferedBody := bufio.NewReader(body)

			textprotoHdr, err := textproto.NewReader(bufferedBody).ReadMIMEHeader()
			if err != nil {
				return nil, err
			}
			parsedHeader := message.Header(textprotoHdr)
			ent, err := message.New(parsedHeader, bufferedBody)
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

func searchOnlyWithFlags(criteria *imap.SearchCriteria) bool {
	if criteria.Header != nil ||
		criteria.Body != nil ||
		criteria.Text != nil ||
		!criteria.SentSince.IsZero() ||
		!criteria.SentBefore.IsZero() ||
		criteria.Smaller != 0 ||
		criteria.Uid != nil ||
		criteria.SeqNum != nil ||
		!criteria.Since.IsZero() ||
		!criteria.Before.IsZero() ||
		criteria.Larger != 0 ||
		criteria.Not != nil ||
		criteria.Or != nil {

		return false
	}

	return true
}

func noSeqNumNeeded(criteria *imap.SearchCriteria) bool {
	if criteria.SeqNum != nil {
		return false
	}

	for _, crit := range criteria.Not {
		if !noSeqNumNeeded(crit) {
			return false
		}
	}
	for _, crit := range criteria.Or {
		if !noSeqNumNeeded(crit[0]) || !noSeqNumNeeded(crit[1]) {
			return false
		}
	}

	return true
}

func (m *Mailbox) flagSearch(uid bool, withFlags, withoutFlags []string) ([]uint32, error) {
	stmt, err := m.getFlagSearchStmt(uid, withFlags, withoutFlags)
	if err != nil {
		return nil, err
	}

	args := m.buildFlagSearchQueryArgs(uid, withFlags, withoutFlags)
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	var res []uint32
	for rows.Next() {
		var id uint32
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		res = append(res, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}
