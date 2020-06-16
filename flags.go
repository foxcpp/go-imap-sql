package imapsql

import (
	"database/sql"
	"strings"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
)

func (m *Mailbox) UpdateMessagesFlags(uid bool, seqset *imap.SeqSet, operation imap.FlagsOp, flags []string) error {
	var err error
	var addQuery, remQuery *sql.Stmt
	switch operation {
	case imap.SetFlags, imap.AddFlags:
		addQuery, err = m.parent.getFlagsAddStmt(uid, flags)
	case imap.RemoveFlags:
		remQuery, err = m.parent.getFlagsRemStmt(uid, flags)
	}
	if err != nil {
		return wrapErr(err, "UpdateMessagesFlags")
	}

	tx, err := m.parent.db.BeginLevel(sql.LevelRepeatableRead, false)
	if err != nil {
		return wrapErr(err, "UpdateMessagesFlags")
	}
	defer tx.Rollback() //nolint:errcheck

	seenModified := false
	newFlagSet := make([]string, 0, len(flags))
	for _, flag := range flags {
		if flag == imap.RecentFlag {
			continue
		}
		if flag == imap.SeenFlag {
			seenModified = true
		}
		newFlagSet = append(newFlagSet, flag)
	}
	flags = newFlagSet

	for _, seq := range seqset.Set {
		start, stop, err := m.resolveSeq(tx, seq, uid)
		if err != nil {
			return wrapErr(err, "UpdateMessagesFlags (resolve seq)")
		}
		m.parent.Opts.Log.Debugln("UpdateMessageFlags: resolved", seq, "to", start, stop, uid)

		switch operation {
		case imap.SetFlags:
			if uid {
				_, err = tx.Stmt(m.parent.massClearFlagsUid).Exec(m.id, start, stop)
			} else {
				_, err = tx.Stmt(m.parent.massClearFlagsSeq).Exec(m.id, m.id, start, stop)
			}
			if err != nil {
				return err
			}
			fallthrough
		case imap.AddFlags:
			args := m.makeFlagsAddStmtArgs(uid, flags, start, stop)
			if _, err := tx.Stmt(addQuery).Exec(args...); err != nil {
				return err
			}
			if seenModified {
				if uid {
					_, err = tx.Stmt(m.parent.setSeenFlagUid).Exec(1, m.id, start, stop)
				} else {
					_, err = tx.Stmt(m.parent.setSeenFlagSeq).Exec(1, m.id, m.id, start, stop)
				}
				if err != nil {
					return err
				}
			}
		case imap.RemoveFlags:
			args := m.makeFlagsRemStmtArgs(uid, flags, start, stop)
			if _, err := tx.Stmt(remQuery).Exec(args...); err != nil {
				return err
			}
			if seenModified {
				if uid {
					_, err = tx.Stmt(m.parent.setSeenFlagUid).Exec(0, m.id, start, stop)
				} else {
					_, err = tx.Stmt(m.parent.setSeenFlagSeq).Exec(0, m.id, m.id, start, stop)
				}
				if err != nil {
					return err
				}
			}
		}
	}

	// We buffer updates before transaction commit so we
	// will not send them if tx.Commit fails.
	updatesBuffer, err := m.flagUpdates(tx, uid, seqset)
	if err != nil {
		return wrapErr(err, "UpdateMessagesFlags")
	}
	m.parent.Opts.Log.Debugln("UpdateMessageFlags: emiting", len(updatesBuffer), "flag updates")

	if err := tx.Commit(); err != nil {
		return wrapErr(err, "UpdateMessagesFlags")
	}

	if m.parent.updates != nil {
		for _, update := range updatesBuffer {
			m.parent.updates <- update
		}
	}
	return nil
}

func (m *Mailbox) flagUpdates(tx *sql.Tx, uid bool, seqset *imap.SeqSet) ([]backend.Update, error) {
	var updatesBuffer []backend.Update

	for _, seq := range seqset.Set {
		var err error
		var rows *sql.Rows
		start, stop, err := m.resolveSeq(tx, seq, uid)
		if err != nil {
			return nil, err
		}

		if uid {
			rows, err = tx.Stmt(m.parent.msgFlagsUid).Query(m.id, m.id, start, stop)
		} else {
			rows, err = tx.Stmt(m.parent.msgFlagsSeq).Query(m.id, m.id, start, stop)
		}
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var seqnum uint32
			var msgId uint32
			var flagsJoined string

			if err := rows.Scan(&seqnum, &msgId, &flagsJoined); err != nil {
				return nil, err
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
			return nil, err
		}
	}

	return updatesBuffer, nil
}
