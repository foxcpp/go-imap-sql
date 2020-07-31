package imapsql

import (
	"database/sql"
	"strings"

	"github.com/emersion/go-imap"
)

func (m *Mailbox) UpdateMessagesFlags(uid bool, seqset *imap.SeqSet, operation imap.FlagsOp, silent bool, flags []string) error {
	defer m.handle.Sync(uid)

	var err error
	var addQuery, remQuery *sql.Stmt
	switch operation {
	case imap.SetFlags, imap.AddFlags:
		addQuery, err = m.parent.getFlagsAddStmt(len(flags))
	case imap.RemoveFlags:
		remQuery, err = m.parent.getFlagsRemStmt(len(flags))
	}
	if err != nil {
		return wrapErr(err, "UpdateMessagesFlags")
	}

	tx, err := m.parent.db.BeginLevel(sql.LevelRepeatableRead, false)
	if err != nil {
		return wrapErr(err, "UpdateMessagesFlags")
	}
	defer tx.Rollback() // nolint:errcheck

	seqset, err = m.handle.ResolveSeq(uid, seqset)
	if err != nil {
		return err
	}

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
		switch operation {
		case imap.SetFlags:
			_, err = tx.Stmt(m.parent.massClearFlagsUid).Exec(m.id, seq.Start, seq.Stop)
			if err != nil {
				return err
			}
			fallthrough
		case imap.AddFlags:
			args := m.makeFlagsAddStmtArgs(flags, seq.Start, seq.Stop)
			if _, err := tx.Stmt(addQuery).Exec(args...); err != nil {
				return err
			}
			if seenModified {
				_, err = tx.Stmt(m.parent.setSeenFlagUid).Exec(1, m.id, seq.Start, seq.Stop)
				if err != nil {
					return err
				}
			}
		case imap.RemoveFlags:
			args := m.makeFlagsRemStmtArgs(flags, seq.Start, seq.Stop)
			if _, err := tx.Stmt(remQuery).Exec(args...); err != nil {
				return err
			}
			if seenModified {
				_, err = tx.Stmt(m.parent.setSeenFlagUid).Exec(0, m.id, seq.Start, seq.Stop)
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

	for _, upd := range updatesBuffer {
		m.handle.FlagsChanged(upd.uid, upd.flags, silent)
	}
	return nil
}

type flagUpdate struct {
	uid   uint32
	flags []string
}

func (m *Mailbox) flagUpdates(tx *sql.Tx, uid bool, seqset *imap.SeqSet) ([]flagUpdate, error) {
	var updatesBuffer []flagUpdate

	for _, seq := range seqset.Set {
		var err error
		var rows *sql.Rows

		rows, err = tx.Stmt(m.parent.msgFlagsUid).Query(m.id, seq.Start, seq.Stop)
		if err != nil {
			return nil, err
		}
		defer rows.Close() // It is fine.

		for rows.Next() {
			var msgId uint32
			var flagsJoined string

			if err := rows.Scan(&msgId, &flagsJoined); err != nil {
				return nil, err
			}

			updatesBuffer = append(updatesBuffer, flagUpdate{
				uid:   msgId,
				flags: strings.Split(flagsJoined, flagsSep),
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		rows.Close()
	}

	return updatesBuffer, nil
}
