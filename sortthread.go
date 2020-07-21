package imapsql

import (
	"database/sql"
	"encoding/json"
	"net/mail"
	"sort"
	"time"

	"github.com/emersion/go-imap"
	sortthread "github.com/emersion/go-imap-sortthread"
)

type msgKey struct {
	ID           uint32
	ArrivalUnix  int64
	BodyLen      uint32
	CachedHeader map[string][]string
}

func (m *Mailbox) Sort(uid bool, sortCrit []sortthread.SortCriterion, searchCrit *imap.SearchCriteria) ([]uint32, error) {
	m.parent.Opts.Log.Debugln("Sort: SORT", uid, sortCrit, searchCrit)
	msgs, err := m.SearchMessages(uid, searchCrit)
	if err != nil {
		return nil, err
	}
	// IDs in msgs are sorted so this will 'compress' adjacent IDs into ranges.
	seqSet := imap.SeqSet{}
	seqSet.AddNum(msgs...)

	// XXX: Split SearchMessages to allow it running in the same transaction.

	tx, err := m.parent.db.BeginLevel(sql.LevelReadCommitted, true)
	if err != nil {
		m.parent.logMboxErr(m, err, "Sort (tx start)", uid, seqSet)
		return nil, err
	}
	defer tx.Rollback()

	resultCount := len(msgs)
	if resultCount > 1000 {
		resultCount = 1000
	}
	sortBuffer := make([]*msgKey, 0, resultCount)

outerLoop:
	for _, seq := range seqSet.Set {
		start, stop, err := m.resolveSeq(tx, seq, uid)
		if err != nil {
			m.parent.logMboxErr(m, err, "Sort (resolve seq)", uid, seqSet)
			return nil, err
		}
		m.parent.Opts.Log.Debugln("Sort: resolved seq", seq, uid, "to", start, stop)

		var rows *sql.Rows
		if uid {
			rows, err = tx.Stmt(m.parent.cachedHeaderUid).Query(m.id, start, stop)
		} else {
			rows, err = tx.Stmt(m.parent.cachedHeaderSeq).Query(m.id, m.id, start, stop)
		}
		if err != nil {
			m.parent.logMboxErr(m, err, "Sort: cachedHeader", uid, seqSet)
			return nil, err
		}

		for rows.Next() {
			var cachedHeaderBlob []byte
			key := msgKey{}
			if err := rows.Scan(&key.ID, &cachedHeaderBlob, &key.BodyLen, &key.ArrivalUnix); err != nil {
				m.parent.logMboxErr(m, err, "Sort: cachedHeader scan", uid, seqSet)
				continue
			}
			if err := json.Unmarshal(cachedHeaderBlob, &key.CachedHeader); err != nil {
				m.parent.logMboxErr(m, err, "Sort: cachedHeader unmarshal", uid, seqSet)
				continue
			}
			sortBuffer = append(sortBuffer, &key)

			// Avoid sorting ass-huge message in memory.
			if len(sortBuffer) == resultCount {
				break outerLoop
			}
		}
	}

	sort.Slice(sortBuffer, messageCompare(sortBuffer, sortCrit))
	ids := make([]uint32, len(sortBuffer))
	for i, msg := range sortBuffer {
		ids[i] = msg.ID /* UID of sequence */
	}
	return ids, nil
}

func firstHeaderField(all []string) string {
	if len(all) > 0 {
		return all[0]
	}
	return ""
}

func firstAddrFromList(all []string) string {
	list, err := mail.ParseAddressList(firstHeaderField(all))
	if err != nil {
		return ""
	}
	if len(list) == 0 {
		return ""
	}
	return list[0].Address
}

func sentDate(dateHeaders []string, arrivalUnix int64) time.Time {
	t, err := mail.ParseDate(firstHeaderField(dateHeaders))
	if err != nil {
		return time.Unix(arrivalUnix, 0)
	}
	return t
}

func messageCompare(buf []*msgKey, sortCrit []sortthread.SortCriterion) func(i, j int) bool {
	return func(i, j int) bool {
		for _, crit := range sortCrit {
			switch crit.Field {
			case "ARRIVAL":
				if crit.Reverse && buf[i].ArrivalUnix > buf[j].ArrivalUnix {
					return true
				} else if buf[i].ArrivalUnix < buf[j].ArrivalUnix {
					return true
				}
			case "CC":
				iAddr := firstAddrFromList(buf[i].CachedHeader["Cc"])
				jAddr := firstAddrFromList(buf[i].CachedHeader["Cc"])
				if crit.Reverse && iAddr > jAddr {
					return true
				} else if iAddr < jAddr {
					return true
				}
			case "DATE":
				iDate := sentDate(buf[i].CachedHeader["Date"], buf[i].ArrivalUnix)
				jDate := sentDate(buf[j].CachedHeader["Date"], buf[j].ArrivalUnix)
				if crit.Reverse && iDate.After(jDate) {
					return true
				} else if iDate.Before(jDate) {
					return true
				}
			case "FROM":
				iAddr := firstAddrFromList(buf[i].CachedHeader["From"])
				jAddr := firstAddrFromList(buf[i].CachedHeader["From"])
				if crit.Reverse && iAddr > jAddr {
					return true
				} else if iAddr < jAddr {
					return true
				}
			case "SIZE":
				if crit.Reverse && buf[i].BodyLen > buf[j].BodyLen {
					return true
				} else if buf[i].BodyLen < buf[j].BodyLen {
					return true
				}
			case "SUBJECT":
				iSubj, _ := sortthread.GetBaseSubject(firstHeaderField(buf[i].CachedHeader["Subject"]))
				jSubj, _ := sortthread.GetBaseSubject(firstHeaderField(buf[j].CachedHeader["Subject"]))
				if crit.Reverse && iSubj > jSubj {
					return true
				} else if iSubj < jSubj {
					return true
				}
			case "TO":
				iAddr := firstAddrFromList(buf[i].CachedHeader["To"])
				jAddr := firstAddrFromList(buf[i].CachedHeader["To"])
				if crit.Reverse && iAddr > jAddr {
					return true
				} else if iAddr < jAddr {
					return true
				}
			}
		}
		return buf[i].ID < buf[j].ID
	}
}
