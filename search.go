package imapsql

import (
	"database/sql"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
	"github.com/emersion/go-message/textproto"
)

func (m *Mailbox) SearchMessages(uid bool, criteria *imap.SearchCriteria) ([]uint32, error) {
	if searchOnlyWithFlags(criteria) {
		if criteria.Not == nil && criteria.Or == nil && criteria.WithFlags == nil && criteria.WithoutFlags == nil {
			return m.allSearch(uid)
		}

		return m.flagSearch(uid, criteria.WithFlags, criteria.WithoutFlags)
	}

	m.handle.ResolveCriteria(criteria)

	needBody := searchNeedsBody(criteria)
	rows, err := m.parent.searchFetchNoSeq.Query(m.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []uint32
	for rows.Next() {
		id, err := m.searchMatches(uid, needBody, rows, criteria)
		if err != nil {
			return nil, err
		}
		if id != 0 {
			res = append(res, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (m *Mailbox) searchMatches(uid, needBody bool, rows *sql.Rows, criteria *imap.SearchCriteria) (uint32, error) {
	var (
		msgId        uint32
		dateUnix     int64
		bodyLen      int
		flagStr      string
		extBodyKey   string
		compressAlgo string
	)

	if err := rows.Scan(&msgId, &dateUnix, &bodyLen, &extBodyKey, &compressAlgo, &flagStr); err != nil {
		return 0, err
	}

	flags := strings.Split(flagStr, flagsSep)
	if len(flags) == 1 && flags[0] == "" {
		flags = nil
	}

	var ent *message.Entity
	var err error
	if needBody {
		bufferedBody, err := m.openBody(true, compressAlgo, extBodyKey)
		if err != nil {
			m.parent.logMboxErr(m, err, "failed to read body, skipping", extBodyKey)
			return 0, nil
		}
		defer bufferedBody.Close()

		hdr, err := textproto.ReadHeader(bufferedBody.Reader)
		if err != nil {
			m.parent.logMboxErr(m, err, "failed to parse body, skipping", extBodyKey)
			return 0, nil
		}

		ent, err = message.New(message.Header{Header: hdr}, bufferedBody.Reader)
		if err != nil {
			m.parent.logMboxErr(m, err, "failed to parse body, skipping", extBodyKey)
			return 0, nil
		}
	} else {
		// XXX: This assumes backendutil.Match will not touch body unless it is needed for criteria.
		ent, _ = message.New(message.Header{}, nil)
	}

	var seqNum uint32
	if !uid {
		var ok bool
		seqNum, ok = m.handle.UidAsSeq(msgId)
		if !ok {
			// Wtf
			return 0, nil
		}
	}

	matched, err := backendutil.Match(ent, seqNum, msgId, time.Unix(dateUnix, 0), flags, criteria)
	if err != nil {
		return 0, err
	}
	if !matched {
		return 0, nil
	}

	if uid {
		return msgId, nil
	} else {
		return seqNum, nil
	}
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

func (m *Mailbox) allSearch(uid bool) ([]uint32, error) {
	if !uid {
		count := m.handle.MsgsCount()
		seqs := make([]uint32, 0, count)
		for i := uint32(1); i <= uint32(count); i++ {
			seqs = append(seqs, i)
		}
		return seqs, nil
	}

	rows, err := m.parent.listMsgUids.Query(m.id)
	if err != nil {
		return nil, err
	}

	var uids []uint32
	for rows.Next() {
		var uid uint32
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}

		uids = append(uids, uid)
	}
	return uids, nil
}

func (m *Mailbox) flagSearch(uid bool, withFlags, withoutFlags []string) ([]uint32, error) {
	recentRequired := false
	recentExcluded := false
	newWithFlags := withFlags[:0]
	for _, f := range withFlags {
		if f == imap.RecentFlag {
			recentRequired = true
			continue
		}
		newWithFlags = append(newWithFlags, f)
	}
	withFlags = newWithFlags
	newWithoutFlags := withoutFlags[:0]
	for _, f := range withoutFlags {
		if f == imap.RecentFlag {
			recentExcluded = true
			continue
		}
		newWithoutFlags = append(newWithoutFlags, f)
	}
	withoutFlags = newWithoutFlags

	stmt, err := m.getFlagSearchStmt(withFlags, withoutFlags)
	if err != nil {
		return nil, err
	}

	args := m.buildFlagSearchQueryArgs(withFlags, withoutFlags)
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

		// Excluding \Recent from SQL-based search will only extend
		// results. Since \Recent is per-connection we cannot use SQL
		// index matching to filter by it, therefore we accept
		// extended results and filter them additionally.
		if recentRequired || recentExcluded {
			if m.handle.IsRecent(id) {
				if recentExcluded {
					continue
				}
			} else if recentRequired {
				continue
			}
		}

		if !uid {
			var ok bool
			id, ok = m.handle.UidAsSeq(id)
			if !ok {
				continue
			}
		}
		res = append(res, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func matchFlags(flags []string, with, without []string) bool {
	flagsSet := make(map[string]bool, len(flags))
	for _, f := range flags {
		flagsSet[f] = true
	}

	for _, f := range without {
		if flagsSet[f] {
			return false
		}
	}
	for _, f := range with {
		if !flagsSet[f] {
			return false
		}
	}

	return true
}
