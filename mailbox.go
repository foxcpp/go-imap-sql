package imapsql

import (
	"bytes"
	"database/sql"
	"io/ioutil"
	"net/textproto"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	appendlimit "github.com/emersion/go-imap-appendlimit"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
	"github.com/foxcpp/go-imap-sql/children"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const flagsSep = "{"

// Message UIDs are assigned sequentelly, starting at 1.

type Mailbox struct {
	user   *User
	uid    uint64
	name   string
	parent *Backend
	id     uint64
}

func (m *Mailbox) Name() string {
	return m.name
}

func (m *Mailbox) Info() (*imap.MailboxInfo, error) {
	res := imap.MailboxInfo{
		Attributes: nil,
		Delimiter:  MailboxPathSep,
		Name:       m.name,
	}
	row := m.parent.getMboxMark.QueryRow(m.uid, m.name)
	mark := 0
	if err := row.Scan(&mark); err != nil {
		return nil, errors.Wrapf(err, "Info %s", m.name)
	}
	if mark == 1 {
		res.Attributes = []string{imap.MarkedAttr}
	}

	if m.parent.childrenExt {
		row = m.parent.hasChildren.QueryRow(m.name+MailboxPathSep+"%", m.uid)
		childrenCount := 0
		if err := row.Scan(&childrenCount); err != nil {
			return nil, errors.Wrapf(err, "Info %s", m.name)
		}
		if childrenCount != 0 {
			res.Attributes = append(res.Attributes, children.HasChildrenAttr)
		} else {
			res.Attributes = append(res.Attributes, children.HasNoChildrenAttr)
		}
	}

	return &res, nil
}

func (m *Mailbox) Status(items []imap.StatusItem) (*imap.MailboxStatus, error) {
	tx, err := m.parent.db.Begin()
	if err != nil {
		return nil, errors.Wrapf(err, "Status %s", m.name)
	}
	defer tx.Rollback() //nolint:errcheck

	res := imap.NewMailboxStatus(m.name, items)
	res.Flags = []string{
		imap.SeenFlag, imap.AnsweredFlag, imap.FlaggedFlag,
		imap.DeletedFlag, imap.DraftFlag,
	}
	res.PermanentFlags = []string{
		imap.SeenFlag, imap.AnsweredFlag, imap.FlaggedFlag,
		imap.DeletedFlag, imap.DraftFlag,
		`\*`,
	}

	rows, err := tx.Stmt(m.parent.usedFlags).Query(m.id)
	if err != nil {
		return nil, errors.Wrapf(err, "Status (usedFlags) %s", m.name)
	}
	for rows.Next() {
		var flag string
		if err := rows.Scan(&flag); err != nil {
			return nil, errors.Wrapf(err, "Status (usedFlags) %s", m.name)
		}
		res.Flags = append(res.Flags, flag)
		res.PermanentFlags = append(res.PermanentFlags, flag)
	}

	row := tx.Stmt(m.parent.firstUnseenSeqNum).QueryRow(m.id, m.id)
	if err := row.Scan(&res.UnseenSeqNum); err != nil {
		if err != sql.ErrNoRows {
			return nil, errors.Wrapf(err, "Status %s", m.name)
		}

		// Don't return it if there is no unseen messages.
		delete(res.Items, imap.StatusUnseen)
		res.UnseenSeqNum = 0
	}

	for _, item := range items {
		switch item {
		case imap.StatusMessages:
			row := tx.Stmt(m.parent.msgsCount).QueryRow(m.id)
			if err := row.Scan(&res.Messages); err != nil {
				return nil, errors.Wrapf(err, "Status (messages) %s", m.name)
			}
		case imap.StatusRecent:
			row := tx.Stmt(m.parent.recentCount).QueryRow(m.id)
			if err := row.Scan(&res.Recent); err != nil {
				return nil, errors.Wrapf(err, "Status (recent) %s", m.name)
			}
		case imap.StatusUidNext:
			res.UidNext, err = m.UidNext(tx)
			if err != nil {
				return nil, errors.Wrapf(err, "Status (uidnext) %s", m.name)
			}
		case imap.StatusUidValidity:
			row := tx.Stmt(m.parent.uidValidity).QueryRow(m.id)
			if err := row.Scan(&res.UidValidity); err != nil {
				return nil, errors.Wrapf(err, "Status (uidvalidity) %s", m.name)
			}
		case appendlimit.StatusAppendLimit:
			val := m.createMessageLimit(tx)
			if val != nil {
				appendlimit.StatusSetAppendLimit(res, val)
			}
		}
	}

	return res, nil
}

func (m *Mailbox) UidNext(tx *sql.Tx) (uint32, error) {
	var row *sql.Row
	if tx != nil {
		row = tx.Stmt(m.parent.uidNext).QueryRow(m.id)
	} else {
		row = m.parent.uidNext.QueryRow(m.id)
	}
	res := sql.NullInt64{}
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	if res.Valid {
		return uint32(res.Int64), nil
	} else {
		return 1, nil
	}
}

func (m *Mailbox) SetSubscribed(subscribed bool) error {
	subbed := 0
	if subscribed {
		subbed = 1
	}
	_, err := m.parent.setSubbed.Exec(subbed, m.id)
	return errors.Wrap(err, "SetSubscribed")
}

func (m *Mailbox) Check() error {
	return nil
}

func (m *Mailbox) createMessageLimit(tx *sql.Tx) *uint32 {
	var res sql.NullInt64
	var row *sql.Row
	if tx == nil {
		row = m.parent.mboxMsgSizeLimit.QueryRow(m.id)
	} else {
		row = tx.Stmt(m.parent.mboxMsgSizeLimit).QueryRow(m.id)
	}
	if err := row.Scan(&res); err != nil {
		return new(uint32) // 0
	}

	if !res.Valid {
		return nil
	} else {
		val := uint32(res.Int64)
		return &val
	}
}

func (m *Mailbox) CreateMessageLimit() *uint32 {
	return m.createMessageLimit(nil)
}

func (m *Mailbox) SetMessageLimit(val *uint32) error {
	_, err := m.parent.setMboxMsgSizeLimit.Exec(val, m.id)
	return err
}

func splitHeader(blob []byte) (header, body []byte) {
	endLen := 4
	headerEnd := bytes.Index(blob, []byte{'\r', '\n', '\r', '\n'})
	if headerEnd == -1 {
		endLen = 2
		headerEnd = bytes.Index(blob, []byte{'\n', '\n'})
		if headerEnd == -1 {
			return nil, blob
		}
	}

	return blob[:headerEnd+endLen], blob[headerEnd+endLen:]
}

func extractCachedData(fullBody []byte) (bodyStructBlob, cachedHeadersBlob []byte, err error) {
	msg, err := message.Read(bytes.NewReader(fullBody))
	if err != nil {
		return nil, nil, err
	}
	hdrs := make(map[string][]string, len(cachedHeaderFields))
	for field, _ := range cachedHeaderFields {
		hdrs[field] = msg.Header[textproto.CanonicalMIMEHeaderKey(field)]
	}

	bodyStruct, err := backendutil.FetchBodyStructure(msg, true)
	if err != nil {
		return nil, nil, err
	}

	bodyStructBlob, err = jsoniter.Marshal(bodyStruct)
	if err != nil {
		return
	}
	cachedHeadersBlob, err = jsoniter.Marshal(hdrs)
	return
}

func (m *Mailbox) CreateMessage(flags []string, date time.Time, fullBody imap.Literal) error {
	mboxLimit := m.CreateMessageLimit()
	if mboxLimit != nil && uint32(fullBody.Len()) > *mboxLimit {
		return appendlimit.ErrTooBig
	} else if mboxLimit == nil {
		userLimit := m.user.CreateMessageLimit()
		if userLimit != nil && uint32(fullBody.Len()) > *userLimit {
			return appendlimit.ErrTooBig
		} else if userLimit == nil {
			if m.parent.opts.MaxMsgBytes != nil && uint32(fullBody.Len()) > *m.parent.opts.MaxMsgBytes {
				return appendlimit.ErrTooBig
			}
		}
	}

	tx, err := m.parent.db.Begin()
	if err != nil {
		return errors.Wrap(err, "CreateMessage (tx begin)")
	}
	defer tx.Rollback() //nolint:errcheck

	msgId, err := m.UidNext(tx)
	if err != nil {
		return errors.Wrap(err, "CreateMessage (uidNext)")
	}

	bodyBlob, err := ioutil.ReadAll(fullBody)
	if err != nil {
		return errors.Wrap(err, "CreateMessage (ReadAll body)")
	}

	hdr, body := splitHeader(bodyBlob)
	bodyStruct, cachedHdr, err := extractCachedData(bodyBlob)
	if err != nil {
		return errors.Wrap(err, "CreateMessage (extractCachedData)")
	}

	_, err = tx.Stmt(m.parent.addMsg).Exec(
		m.id, msgId, date.Unix(),
		len(hdr), hdr, len(body), body,
		bodyStruct, cachedHdr,
	)
	if err != nil {
		return errors.Wrap(err, "CreateMessage (addMsg)")
	}

	haveRecent := false
	for _, flag := range flags {
		if flag == imap.RecentFlag {
			haveRecent = true
		}
	}
	if !haveRecent {
		flags = append(flags, imap.RecentFlag)
	}

	if len(flags) != 0 {
		// TOOD: Use addFlag if only one flag is added.
		flagsReq := m.parent.db.rewriteSQL(`
			INSERT INTO flags
			SELECT ?, msgId, column1 AS flag
			FROM msgs
			CROSS JOIN (` + m.valuesSubquery(flags) + `) flagset
			WHERE mboxId = ? AND msgId = ?
			ON CONFLICT DO NOTHING`)

		// How horrible variable arguments in Go are...
		params := make([]interface{}, 0, 3+len(flags))
		params = append(params, m.id)
		for _, flag := range flags {
			params = append(params, flag)
		}
		params = append(params, m.id, msgId)
		if _, err := tx.Exec(flagsReq, params...); err != nil {
			return errors.Wrap(err, "CreateMessage (flags)")
		}
	}

	if _, err := tx.Stmt(m.parent.addUidNext).Exec(1, m.id); err != nil {
		return errors.Wrap(err, "CreateMessage (uidnext bump)")
	}

	upd, err := m.statusUpdate(tx)
	if err != nil {
		return errors.Wrap(err, "CreateMessage (status query)")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "CreateMessage (tx commit)")
	}

	// Send update after commiting transaction,
	// just in case reading side will block us for some time.
	if m.parent.updates != nil {
		m.parent.updates <- upd
	}
	return nil
}

func (m *Mailbox) statusUpdate(tx *sql.Tx) (backend.Update, error) {
	row := tx.Stmt(m.parent.msgsCount).QueryRow(m.id)
	newCount := uint32(0)
	if err := row.Scan(&newCount); err != nil {
		return nil, errors.Wrap(err, "CreateMessage (exists read)")
	}

	row = tx.Stmt(m.parent.recentCount).QueryRow(m.id)
	newRecent := uint32(0)
	if err := row.Scan(&newRecent); err != nil {
		return nil, errors.Wrap(err, "CreateMessage (recent read)")
	}

	upd := backend.MailboxUpdate{
		Update:        backend.NewUpdate(m.user.username, m.name),
		MailboxStatus: imap.NewMailboxStatus(m.name, []imap.StatusItem{imap.StatusMessages, imap.StatusRecent}),
	}
	upd.MailboxStatus.Messages = newCount
	upd.MailboxStatus.Recent = newRecent

	return &upd, nil
}

func (m *Mailbox) UpdateMessagesFlags(uid bool, seqset *imap.SeqSet, operation imap.FlagsOp, flags []string) error {
	tx, err := m.parent.db.Begin()
	if err != nil {
		return errors.Wrap(err, "UpdateMessagesFlags")
	}
	defer tx.Rollback() //nolint:errcheck

	var query *sql.Stmt

	newFlagSet := make([]string, 0, len(flags))
	for _, flag := range flags {
		if flag == imap.RecentFlag {
			continue
		}
		newFlagSet = append(newFlagSet, flag)
	}
	flags = newFlagSet

	switch operation {
	case imap.SetFlags:
		for _, seq := range seqset.Set {
			start, stop := sqlRange(seq)
			if uid {
				_, err = tx.Stmt(m.parent.massClearFlagsUid).Exec(m.id, start, stop)
			} else {
				_, err = tx.Stmt(m.parent.massClearFlagsSeq).Exec(m.id, m.id, start, stop)
			}
			if err != nil {
				return errors.Wrap(err, "UpdateMessagesFlags")
			}
		}
		fallthrough
	case imap.AddFlags:
		if uid {
			query, err = tx.Prepare(m.parent.db.rewriteSQL(`
				INSERT INTO flags
				SELECT ? AS mboxId, msgId, column1 AS flag
				FROM msgs
				CROSS JOIN (` + m.valuesSubquery(flags) + `) flagset
				WHERE mboxId = ? AND msgId BETWEEN ? AND ?
				ON CONFLICT DO NOTHING`))
		} else {
			// ON 1=1 is necessary to make SQLite's parser not interpret ON CONFLICT as join condition.
			if m.parent.db.driver == "sqlite3" {
				query, err = tx.Prepare(m.parent.db.rewriteSQL(`
					INSERT INTO flags
					SELECT ? AS mboxId, msgId, column1 AS flag
					FROM (SELECT msgId FROM msgs WHERE mboxId = ? LIMIT ? OFFSET ?) msgIds
					CROSS JOIN (` + m.valuesSubquery(flags) + `) flagset ON 1=1
					ON CONFLICT DO NOTHING`))
			} else {
				// But 1 = 1 in query causes errors on PostgreSQL.
				query, err = tx.Prepare(m.parent.db.rewriteSQL(`
					INSERT INTO flags
					SELECT ? AS mboxId, msgId, column1 AS flag
					FROM (SELECT msgId FROM msgs WHERE mboxId = ? LIMIT ? OFFSET ?) msgIds
					CROSS JOIN (` + m.valuesSubquery(flags) + `) flagset
					ON CONFLICT DO NOTHING`))
			}
		}
		if err != nil {
			return errors.Wrap(err, "UpdateMessagesFlags")
		}

		for _, seq := range seqset.Set {
			start, stop := sqlRange(seq)

			// How horrible variable arguments in Go are...
			if uid {
				params := make([]interface{}, 0, 4+len(flags))
				params = append(params, m.id)
				for _, flag := range flags {
					params = append(params, flag)
				}
				params = append(params, m.id, start, stop)

				_, err = query.Exec(params...)
			} else {
				params := make([]interface{}, 0, 4+len(flags))
				params = append(params, m.id, m.id, stop-start+1, start-1)
				for _, flag := range flags {
					params = append(params, flag)
				}

				_, err = query.Exec(params...)
			}
			if err != nil {
				query.Close()
				return errors.Wrap(err, "UpdateMessagesFlags")
			}
		}
		query.Close()
	case imap.RemoveFlags:
		if uid {
			query, err = tx.Prepare(m.parent.db.rewriteSQL(`
				DELETE FROM flags
				WHERE mboxId = ?
				AND msgId BETWEEN ? AND ?
				AND flag IN (` + m.valuesSubquery(flags) + `)`))
		} else {
			query, err = tx.Prepare(m.parent.db.rewriteSQL(`
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
				) AND flag IN (` + m.valuesSubquery(flags) + `)`))
		}
		if err != nil {
			return errors.Wrap(err, "UpdateMessagesFlags")
		}

		for _, seq := range seqset.Set {
			start, stop := sqlRange(seq)
			if uid {
				params := make([]interface{}, 0, 3+len(flags))
				params = append(params, m.id, start, stop)
				for _, flag := range flags {
					params = append(params, flag)
				}
				_, err = query.Exec(params...)
			} else {
				params := make([]interface{}, 0, 4+len(flags))
				params = append(params, m.id, m.id, start, stop)
				for _, flag := range flags {
					params = append(params, flag)
				}
				_, err = query.Exec(params...)
			}
			if err != nil {
				query.Close()
				return errors.Wrap(err, "UpdateMessagesFlags")
			}
		}
		query.Close()
	}

	// We buffer updates before transaction commit so we
	// will not send them if tx.Commit fails.
	var updatesBuffer []backend.Update

	for _, seq := range seqset.Set {
		var err error
		var rows *sql.Rows
		start, stop := sqlRange(seq)

		if uid {
			rows, err = tx.Stmt(m.parent.msgFlagsUid).Query(m.id, m.id, start, stop)
		} else {
			rows, err = tx.Stmt(m.parent.msgFlagsSeq).Query(m.id, m.id, start, stop)
		}
		if err != nil {
			return errors.Wrap(err, "UpdateMessagesFlags")
		}

		for rows.Next() {
			var seqnum uint32
			var msgId uint32
			var flagsJoined string

			if err := rows.Scan(&seqnum, &msgId, &flagsJoined); err != nil {
				return errors.Wrap(err, "UpdateMessagesFlags")
			}

			flags := strings.Split(flagsJoined, flagsSep)

			updatesBuffer = append(updatesBuffer, &backend.MessageUpdate{
				Update: backend.NewUpdate(m.user.username, m.name),
				Message: &imap.Message{
					SeqNum: seqnum,
					Items:  map[imap.FetchItem]interface{}{imap.FetchFlags: nil},
					Flags:  flags,
				},
			})
		}
		if err := rows.Err(); err != nil {
			return errors.Wrap(err, "UpdateMessagesFlags")
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "UpdateMessagesFlags")
	}

	if m.parent.updates != nil {
		for _, update := range updatesBuffer {
			m.parent.updates <- update
		}
	}
	return nil
}

func (m *Mailbox) valuesSubquery(rows []string) string {
	count := len(rows)
	sqlList := ""
	if m.parent.db.driver == "mysql" {

		sqlList += "SELECT ? AS column1"
		for i := 1; i < count; i++ {
			sqlList += " UNION ALL SELECT ? "
		}

		return sqlList
	}

	for i := 0; i < count; i++ {
		sqlList += "(?)"
		if i+1 != count {
			sqlList += ","
		}
	}

	return "VALUES " + sqlList
}

func (m *Mailbox) MoveMessages(uid bool, seqset *imap.SeqSet, dest string) error {
	tx, err := m.parent.db.Begin()
	if err != nil {
		return errors.Wrap(err, "MoveMessages")
	}
	defer tx.Rollback() //nolint:errcheck

	updatesBuffer := make([]backend.Update, 0, 16)

	if err := m.copyMessages(tx, uid, seqset, dest, &updatesBuffer); err != nil {
		if err == backend.ErrNoSuchMailbox {
			return err
		}
		return errors.Wrap(err, "MoveMessages")
	}
	if err := m.delMessages(tx, uid, seqset, &updatesBuffer); err != nil {
		return errors.Wrap(err, "MoveMessages")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "MoveMessages")
	}

	if m.parent.updates != nil {
		for _, upd := range updatesBuffer {
			m.parent.updates <- upd
		}
	}
	return nil
}

func (m *Mailbox) CopyMessages(uid bool, seqset *imap.SeqSet, dest string) error {
	tx, err := m.parent.db.Begin()
	if err != nil {
		return errors.Wrap(err, "CopyMessages")
	}
	defer tx.Rollback() //nolint:errcheck

	updatesBuffer := make([]backend.Update, 0, 16)

	if err := m.copyMessages(tx, uid, seqset, dest, &updatesBuffer); err != nil {
		if err == backend.ErrNoSuchMailbox {
			return err
		}
		return errors.Wrap(err, "CopyMessages")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "CopyMessages")
	}

	if m.parent.updates != nil {
		for _, upd := range updatesBuffer {
			m.parent.updates <- upd
		}
	}
	return nil
}

func (m *Mailbox) DelMessages(uid bool, seqset *imap.SeqSet) error {
	tx, err := m.parent.db.Begin()
	if err != nil {
		return errors.Wrap(err, "DelMessages")
	}
	defer tx.Rollback() //nolint:errcheck

	updatesBuffer := make([]backend.Update, 0, 16)
	if err := m.delMessages(tx, uid, seqset, &updatesBuffer); err != nil {
		if err == backend.ErrNoSuchMailbox {
			return err
		}
		return errors.Wrap(err, "DelMessages")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "DelMessages")
	}

	if m.parent.updates != nil {
		for _, upd := range updatesBuffer {
			m.parent.updates <- upd
		}
	}
	return nil
}

func (m *Mailbox) delMessages(tx *sql.Tx, uid bool, seqset *imap.SeqSet, updsBuffer *[]backend.Update) error {
	for _, seq := range seqset.Set {
		start, stop := sqlRange(seq)

		var err error
		if uid {
			_, err = tx.Stmt(m.parent.markUid).Exec(m.id, start, stop)
		} else {
			_, err = tx.Stmt(m.parent.markSeq).Exec(m.id, m.id, start, stop)
		}
		if err != nil {
			return err
		}
	}

	rows, err := tx.Stmt(m.parent.markedSeqnums).Query(m.id)
	if err != nil {
		return err
	}
	for rows.Next() {
		var seqnum uint32
		if err := rows.Scan(&seqnum); err != nil {
			return err
		}

		*updsBuffer = append(*updsBuffer, &backend.ExpungeUpdate{
			Update: backend.NewUpdate(m.user.username, m.name),
			SeqNum: seqnum,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	_, err = tx.Stmt(m.parent.delMarked).Exec()
	return err
}

func (m *Mailbox) copyMessages(tx *sql.Tx, uid bool, seqset *imap.SeqSet, dest string, updsBuffer *[]backend.Update) error {
	destID := uint64(0)
	row := tx.Stmt(m.parent.mboxId).QueryRow(m.uid, dest)
	if err := row.Scan(&destID); err != nil {
		if err == sql.ErrNoRows {
			return backend.ErrNoSuchMailbox
		}
	}

	destMbox := Mailbox{user: m.user, id: destID, name: dest, parent: m.parent}

	srcId := m.id

	for _, seq := range seqset.Set {
		start, stop := sqlRange(seq)

		var stats sql.Result
		var err error
		if uid {
			stats, err = tx.Stmt(m.parent.copyMsgsUid).Exec(destID, destID, srcId, start, stop)
			if err != nil {
				return err
			}
			if _, err := tx.Stmt(m.parent.copyMsgFlagsUid).Exec(destID, destID, srcId, start, stop); err != nil {
				return err
			}
		} else {
			stats, err = tx.Stmt(m.parent.copyMsgsSeq).Exec(destID, destID, srcId, stop-start+1, start-1)
			if err != nil {
				return err
			}
			if _, err := tx.Stmt(m.parent.copyMsgFlagsSeq).Exec(destID, destID, srcId, stop-start+1, start-1); err != nil {
				return err
			}
		}
		affected, err := stats.RowsAffected()
		if err != nil {
			return err
		}

		if _, err := tx.Stmt(m.parent.addRecentToLast).Exec(destID, destID, affected); err != nil {
			return err
		}

		if _, err := tx.Stmt(m.parent.addUidNext).Exec(affected, destID); err != nil {
			return err
		}
	}

	upd, err := destMbox.statusUpdate(tx)
	if err != nil {
		return err
	}
	*updsBuffer = append(*updsBuffer, upd)

	return nil
}

func (m *Mailbox) Expunge() error {
	tx, err := m.parent.db.Begin()
	if err != nil {
		return errors.Wrap(err, "Expunge")
	}
	defer tx.Rollback() //nolint:errcheck

	var seqnums []uint32
	// Query returns seqnum in reversed order.
	rows, err := tx.Stmt(m.parent.deletedSeqnums).Query(m.id, m.id)
	if err != nil {
		return errors.Wrap(err, "Expunge")
	}
	for rows.Next() {
		var seqnum uint32
		if err := rows.Scan(&seqnum); err != nil {
			return errors.Wrap(err, "Expunge")
		}
		seqnums = append(seqnums, seqnum)
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "Expunge")
	}

	_, err = tx.Stmt(m.parent.expungeMbox).Exec(m.id, m.id)
	if err != nil {
		return errors.Wrap(err, "Expunge")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "Expunge")
	}

	if m.parent.updates != nil {
		for _, seqnum := range seqnums {
			m.parent.updates <- &backend.ExpungeUpdate{
				Update: backend.NewUpdate(m.user.username, m.name),
				SeqNum: seqnum,
			}
		}
	}

	return nil
}

func sqlRange(seq imap.Seq) (x, y uint32) {
	x = seq.Start
	y = seq.Stop
	if seq.Stop == 0 {
		y = 4294967295
	}
	if seq.Start == 0 {
		x = 1
	}
	return
}
