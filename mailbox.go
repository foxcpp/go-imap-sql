package imapsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	appendlimit "github.com/emersion/go-imap-appendlimit"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message/textproto"
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
	row := m.parent.getMboxAttrs.QueryRow(m.uid, m.name)
	var mark int
	var specialUse sql.NullString
	if err := row.Scan(&mark, &specialUse); err != nil {
		return nil, errors.Wrapf(err, "Info %s", m.name)
	}
	if mark == 1 {
		res.Attributes = []string{imap.MarkedAttr}
	}
	if specialUse.Valid && m.parent.specialUseExt {
		res.Attributes = []string{specialUse.String}
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
			res.UidNext, err = m.uidNext(tx)
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

func (m *Mailbox) uidNext(tx *sql.Tx) (uint32, error) {
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

func extractCachedData(hdr textproto.Header, bufferedBody *bufio.Reader) (bodyStructBlob, cachedHeadersBlob []byte, err error) {
	hdrs := make(map[string][]string, len(cachedHeaderFields))
	for field := hdr.Fields(); field.Next(); {
		if _, ok := cachedHeaderFields[strings.ToLower(field.Key())]; !ok {
			continue
		}
		hdrs[strings.ToLower(field.Key())] = append(hdrs[strings.ToLower(field.Key())], field.Value())
	}

	bodyStruct, err := backendutil.FetchBodyStructure(hdr, bufferedBody, true)
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

func (b *Backend) processBody(literal imap.Literal) (headerBlob, bodyBlob, bodyStruct, cachedHeader []byte, extBodyKey sql.NullString, err error) {
	var bodyReader io.Reader = literal
	if b.Opts.ExternalStore != nil {
		extBodyKey.String, err = randomKey()
		if err != nil {
			return nil, nil, nil, nil, sql.NullString{}, err
		}
		extBodyKey.Valid = true
		extWriter, err := b.Opts.ExternalStore.Create(extBodyKey.String)
		if err != nil {
			return nil, nil, nil, nil, sql.NullString{}, err
		}
		defer extWriter.Close()

		bodyReader = io.TeeReader(literal, extWriter)

		headerBlob = nil
		bodyBlob = nil
	} else {
		bodyBuf, err := ioutil.ReadAll(literal)
		if err != nil {
			return nil, nil, nil, nil, sql.NullString{}, errors.Wrap(err, "CreateMessage (ReadAll body)")
		}
		headerBlob, bodyBlob = splitHeader(bodyBuf)
		bodyReader = bytes.NewReader(bodyBuf)
	}

	bufferedBody := bufio.NewReader(bodyReader)
	hdr, err := textproto.ReadHeader(bufferedBody)
	if err != nil {
		return nil, nil, nil, nil, sql.NullString{}, errors.Wrap(err, "CreateMessage (readHeader)")
	}

	bodyStruct, cachedHeader, err = extractCachedData(hdr, bufferedBody)
	if err != nil {
		return nil, nil, nil, nil, sql.NullString{}, errors.Wrap(err, "CreateMessage (extractCachedData)")
	}

	// Consume all remaining body so io.TeeReader used with external store will
	// copy everything to extWriter.
	_, err = io.Copy(ioutil.Discard, bufferedBody)
	if err != nil {
		return nil, nil, nil, nil, sql.NullString{}, errors.Wrap(err, "CreateMessage (ReadAll consume)")
	}

	return
}

func (m *Mailbox) checkAppendLimit(length int) error {
	mboxLimit := m.CreateMessageLimit()
	if mboxLimit != nil && uint32(length) > *mboxLimit {
		return appendlimit.ErrTooBig
	} else if mboxLimit == nil {
		userLimit := m.user.CreateMessageLimit()
		if userLimit != nil && uint32(length) > *userLimit {
			return appendlimit.ErrTooBig
		} else if userLimit == nil {
			if m.parent.Opts.MaxMsgBytes != nil && uint32(length) > *m.parent.Opts.MaxMsgBytes {
				return appendlimit.ErrTooBig
			}
		}
	}
	return nil
}

func (m *Mailbox) CreateMessage(flags []string, date time.Time, fullBody imap.Literal) error {
	if err := m.checkAppendLimit(fullBody.Len()); err != nil {
		return err
	}

	// Important to run before transaction, otherwise it will deadlock on
	// SQLite.
	haveRecent := false
	haveSeen := false
	for _, flag := range flags {
		if flag == imap.RecentFlag {
			haveRecent = true
		}
		if flag == imap.SeenFlag {
			haveSeen = true
		}
	}
	if !haveRecent {
		flags = append(flags, imap.RecentFlag)
	}
	stmt, err := m.parent.getFlagsAddStmt(true, flags)
	if err != nil {
		return errors.Wrap(err, "CreateMessage")
	}

	tx, err := m.parent.db.Begin()
	if err != nil {
		return errors.Wrap(err, "CreateMessage (tx begin)")
	}
	defer tx.Rollback() //nolint:errcheck

	msgId, err := m.uidNext(tx)
	if err != nil {
		return errors.Wrap(err, "CreateMessage (uidNext)")
	}

	bodyLen := fullBody.Len()
	hdr, body, bodyStruct, cachedHdr, extBodyKey, err := m.parent.processBody(fullBody)
	if err != nil {
		return err
	}

	if extBodyKey.Valid {
		if _, err = tx.Stmt(m.parent.addExtKey).Exec(extBodyKey, 1); err != nil {
			return errors.Wrap(err, "CreateMessage (addExtKey)")
		}
	}

	_, err = tx.Stmt(m.parent.addMsg).Exec(
		m.id, msgId, date.Unix(),
		bodyLen, body, hdr,
		bodyStruct, cachedHdr, extBodyKey,
		haveSeen,
	)
	if err != nil {
		return errors.Wrap(err, "CreateMessage (addMsg)")
	}

	params := m.makeFlagsAddStmtArgs(true, flags, imap.Seq{msgId, msgId})
	if _, err := tx.Stmt(stmt).Exec(params...); err != nil {
		return errors.Wrap(err, "CreateMessage (flags)")
	}

	if _, err := tx.Stmt(m.parent.increaseMsgCount).Exec(1, 1, m.id); err != nil {
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
	upd := backend.MailboxUpdate{
		Update:        backend.NewUpdate(m.user.username, m.name),
		MailboxStatus: imap.NewMailboxStatus(m.name, []imap.StatusItem{imap.StatusMessages, imap.StatusUnseen}),
	}

	row := tx.Stmt(m.parent.msgsCount).QueryRow(m.id)
	newCount := uint32(0)
	if err := row.Scan(&newCount); err != nil {
		return nil, errors.Wrap(err, "CreateMessage (exists read)")
	}

	unseenSeqNum := uint32(0)
	row = tx.Stmt(m.parent.firstUnseenSeqNum).QueryRow(m.id, m.id)
	if err := row.Scan(&unseenSeqNum); err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrap(err, "CreateMessage (unseen seq num read)")
	}
	if unseenSeqNum == 0 {
		delete(upd.MailboxStatus.Items, imap.StatusUnseen)
	}

	upd.MailboxStatus.Flags = nil
	upd.MailboxStatus.PermanentFlags = nil
	upd.MailboxStatus.UnseenSeqNum = unseenSeqNum
	upd.MailboxStatus.Messages = newCount

	return &upd, nil
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

	var deletedExtKeys []string

	rows, err := tx.Stmt(m.parent.markedSeqnums).Query(m.id)
	if err != nil {
		return err
	}
	for rows.Next() {
		var seqnum uint32
		var extKey sql.NullString
		if err := rows.Scan(&seqnum, &extKey); err != nil {
			return err
		}

		if extKey.Valid {
			deletedExtKeys = append(deletedExtKeys, extKey.String)
		}
		*updsBuffer = append(*updsBuffer, &backend.ExpungeUpdate{
			Update: backend.NewUpdate(m.user.username, m.name),
			SeqNum: seqnum,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if m.parent.Opts.ExternalStore != nil {
		if err := m.parent.Opts.ExternalStore.Delete(deletedExtKeys); err != nil {
			return err
		}
	}

	if _, err := tx.Stmt(m.parent.delMarked).Exec(); err != nil {
		return err
	}

	_, err = tx.Stmt(m.parent.decreaseMsgCount).Exec(len(*updsBuffer), m.id)
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

		if m.parent.Opts.ExternalStore != nil {
			if uid {
				if _, err := tx.Stmt(m.parent.incrementRefUid).Exec(srcId, start, stop); err != nil {
					return err
				}
			} else {
				if _, err := tx.Stmt(m.parent.incrementRefSeq).Exec(srcId, srcId, start, stop); err != nil {
					return err
				}
			}
		}

		if _, err := tx.Stmt(m.parent.addRecentToLast).Exec(destID, destID, affected); err != nil {
			return err
		}

		if _, err := tx.Stmt(m.parent.increaseMsgCount).Exec(affected, affected, destID); err != nil {
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

	if m.parent.Opts.ExternalStore != nil {
		if err := m.expungeExternal(tx); err != nil {
			return err
		}
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

func (m *Mailbox) expungeExternal(tx *sql.Tx) error {
	if _, err := tx.Stmt(m.parent.decreaseRefForDeleted).Exec(m.id); err != nil {
		return err
	}

	rows, err := tx.Stmt(m.parent.zeroRef).Query(m.id)
	if err != nil {
		return err
	}
	defer rows.Close()

	keys := make([]string, 0, 16)
	for rows.Next() {
		var extKey string
		if err := rows.Scan(&extKey); err != nil {
			return err
		}
		keys = append(keys, extKey)

	}

	if err := m.parent.Opts.ExternalStore.Delete(keys); err != nil {
		return err
	}
	if _, err := tx.Stmt(m.parent.deleteZeroRef).Exec(); err != nil {
		return err
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
