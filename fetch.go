package imapsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message/textproto"
	"github.com/pkg/errors"
)

func (m *Mailbox) ListMessages(uid bool, seqset *imap.SeqSet, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer close(ch)
	var err error

	setSeen := shouldSetSeen(items)
	var addSeenStmt *sql.Stmt
	if setSeen {
		addSeenStmt, err = m.parent.getFlagsAddStmt(uid, []string{imap.SeenFlag})
		if err != nil {
			return err
		}

		// Duplicate entries (if any) shouldn't cause problems.
		items = append(items, imap.FetchFlags)
	}

	stmt, err := m.parent.getFetchStmt(uid, items)
	if err != nil {
		return err
	}

	// don't close statement, it is owned by cache
	tx, err := m.parent.db.BeginLevel(sql.LevelReadCommitted, !setSeen)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, seq := range seqset.Set {
		begin, end := sqlRange(seq)

		if setSeen {
			params := m.makeFlagsAddStmtArgs(uid, []string{imap.SeenFlag}, seq)
			if _, err := tx.Stmt(addSeenStmt).Exec(params...); err != nil {
				return err
			}

			start, stop := sqlRange(seq)
			if uid {
				_, err = tx.Stmt(m.parent.setSeenFlagUid).Exec(1, m.id, start, stop)
			} else {
				_, err = tx.Stmt(m.parent.setSeenFlagSeq).Exec(1, m.id, m.id, start, stop)
			}
		}

		rows, err := tx.Stmt(stmt).Query(m.id, m.id, begin, end)
		if err != nil {
			return err
		}
		if err := m.scanMessages(rows, items, ch); err != nil {
			return err
		}
	}

	return nil
}

type scanData struct {
	cachedHeaderBlob, bodyStructureBlob []byte

	seqNum, msgId uint32
	dateUnix      int64
	bodyLen       uint32
	flagStr       string
	extBodyKey    sql.NullString

	bodyReader    io.ReadCloser
	bodyStructure *imap.BodyStructure
	cachedHeader  map[string][]string
	parsedHeader  *textproto.Header
}

func makeScanArgs(data *scanData, rows *sql.Rows) ([]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	scanOrder := make([]interface{}, 0, len(cols))
	for _, col := range cols {
		// PostgreSQL case-folds column names to lower-case.
		switch col {
		case "seqnum":
			scanOrder = append(scanOrder, &data.seqNum)
		case "date":
			scanOrder = append(scanOrder, &data.dateUnix)
		case "bodyLen", "bodylen":
			scanOrder = append(scanOrder, &data.bodyLen)
		case "msgId", "msgid":
			scanOrder = append(scanOrder, &data.msgId)
		case "cachedHeader", "cachedheader":
			scanOrder = append(scanOrder, &data.cachedHeaderBlob)
		case "bodyStructure", "bodystructure":
			scanOrder = append(scanOrder, &data.bodyStructureBlob)
		case "extBodyKey", "extbodykey":
			scanOrder = append(scanOrder, &data.extBodyKey)
		case "flags":
			scanOrder = append(scanOrder, &data.flagStr)
		default:
			panic("unknown column: " + col)
		}
	}

	return scanOrder, nil
}

func (m *Mailbox) scanMessages(rows *sql.Rows, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer rows.Close()
	data := scanData{}

	scanArgs, err := makeScanArgs(&data, rows)
	if err != nil {
		return err
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		data.parsedHeader = nil
		data.cachedHeader = nil
		data.bodyStructure = nil

		if data.cachedHeaderBlob != nil {
			if err := json.Unmarshal(data.cachedHeaderBlob, &data.cachedHeader); err != nil {
				return err
			}
		}
		if data.bodyStructureBlob != nil {
			if err := json.Unmarshal(data.bodyStructureBlob, &data.bodyStructure); err != nil {
				return err
			}
		}

		msg := imap.NewMessage(data.seqNum, items)
		for _, item := range items {
			switch item {
			case imap.FetchInternalDate:
				msg.InternalDate = time.Unix(data.dateUnix, 0)
			case imap.FetchRFC822Size:
				msg.Size = data.bodyLen
			case imap.FetchUid:
				msg.Uid = data.msgId
			case imap.FetchEnvelope:
				raw := envelopeFromHeader(data.cachedHeader)
				msg.Envelope = raw.toIMAP()
			case imap.FetchBody:
				msg.BodyStructure = stripExtBodyStruct(data.bodyStructure)
			case imap.FetchBodyStructure:
				msg.BodyStructure = data.bodyStructure
			case imap.FetchFlags:
				msg.Flags = strings.Split(data.flagStr, flagsSep)
			default:
				if err := m.extractBodyPart(item, &data, msg); err != nil {
					return err
				}
			}
		}

		ch <- msg
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (m *Mailbox) extractBodyPart(item imap.FetchItem, data *scanData, msg *imap.Message) error {
	sect, part, err := getNeededPart(item)
	if err != nil {
		return err
	}

	switch part {
	case needCachedHeader:
		var err error
		msg.Body[sect], err = headerSubsetFromCached(sect, data.cachedHeader)
		if err != nil {
			return err
		}
	case needHeader, needFullBody:
		// We don't need to parse header once more if we already did, so we just skip it if we open body
		// multiple times.
		bufferedBody, err := m.openBody(data.parsedHeader == nil, data.extBodyKey)
		if err != nil {
			return err
		}
		defer bufferedBody.Close()

		if data.parsedHeader == nil {
			hdr, err := textproto.ReadHeader(bufferedBody.Reader)
			if err != nil {
				return err
			}
			data.parsedHeader = &hdr
		}

		msg.Body[sect], err = backendutil.FetchBodySection(*data.parsedHeader, bufferedBody.Reader, sect)
		if err != nil {
			return err
		}
	}

	return nil
}

type BufferedReadCloser struct {
	*bufio.Reader
	io.Closer
}

type nopCloser struct{}

func (n nopCloser) Close() error {
	return nil
}

func (m *Mailbox) openBody(needHeader bool, extBodyKey sql.NullString) (BufferedReadCloser, error) {
	if m.parent.extStore == nil {
		return BufferedReadCloser{}, errors.New("DB entry references External Storage, but no Storage was configured")
	}
	rdr, err := m.parent.extStore.Open(extBodyKey.String)
	if err != nil {
		return BufferedReadCloser{}, err
	}

	bufR := bufio.NewReader(rdr)
	if !needHeader {
		for {
			// Skip header if it is not needed.
			line, err := bufR.ReadSlice('\n')
			if err != nil {
				return BufferedReadCloser{}, err
			}
			// If line is empty (message uses LF delim) or contains only CR (messages uses CRLF delim)
			if len(line) == 0 || (len(line) == 1 || line[0] == '\r') {
				break
			}
		}
	}

	return BufferedReadCloser{Reader: bufR, Closer: rdr}, nil
}

func headerSubsetFromCached(sect *imap.BodySectionName, cachedHeader map[string][]string) (imap.Literal, error) {
	hdr := textproto.Header{}
	for i := len(sect.Fields) - 1; i >= 0; i-- {
		field := sect.Fields[i]

		value := cachedHeader[strings.ToLower(field)]
		for i := len(value) - 1; i >= 0; i-- {
			subval := value[i]
			hdr.Add(field, subval)
		}
	}

	buf := new(bytes.Buffer)
	if err := textproto.WriteHeader(buf, hdr); err != nil {
		return nil, err
	}

	var l imap.Literal = buf
	if sect.Partial != nil {
		l = bytes.NewReader(sect.ExtractPartial(buf.Bytes()))
	}

	return l, nil
}

func stripExtBodyStruct(extended *imap.BodyStructure) *imap.BodyStructure {
	stripped := *extended
	stripped.Extended = false
	stripped.Disposition = ""
	stripped.DispositionParams = nil
	stripped.Language = nil
	stripped.Location = nil
	stripped.MD5 = ""

	for i := range stripped.Parts {
		stripped.Parts[i] = stripExtBodyStruct(stripped.Parts[i])
	}
	return &stripped
}

func shouldSetSeen(items []imap.FetchItem) bool {
	for _, item := range items {
		switch item {
		case imap.FetchInternalDate, imap.FetchRFC822Size, imap.FetchUid, imap.FetchEnvelope,
			imap.FetchBody, imap.FetchBodyStructure, imap.FetchFlags:
			continue
		default:
			sect, err := imap.ParseBodySectionName(item)
			if err != nil {
				return false
			}
			if !sect.Peek {
				return true
			}
		}
	}
	return false
}
