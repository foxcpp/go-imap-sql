package imapsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"io/ioutil"
	"strings"
	"time"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
	"github.com/emersion/go-message/textproto"
	jsoniter "github.com/json-iterator/go"
)

func (m *Mailbox) ListMessages(uid bool, seqset *imap.SeqSet, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer close(ch)

	stmt, err := m.parent.getFetchStmt(uid, items)
	if err != nil {
		return err
	}

	// don't close statement, it is owned by cache
	tx, err := m.parent.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, seq := range seqset.Set {
		begin, end := sqlRange(seq)

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
	cachedHeaderBlob, bodyStructureBlob, headerBlob, bodyBlob []byte

	seqNum, msgId uint32
	dateUnix      int64
	bodyLen       uint32
	flagStr       string
	extBodyKey    sql.NullString

	bodyReader    io.ReadCloser
	bodyStructure *imap.BodyStructure
	cachedHeader  map[string][]string
	parsedHeader  *message.Header
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
		case "header":
			scanOrder = append(scanOrder, &data.headerBlob)
		case "body":
			scanOrder = append(scanOrder, &data.bodyBlob)
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
	data := scanData{bodyLen: 15}

	scanArgs, err := makeScanArgs(&data, rows)
	if err != nil {
		return err
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		if data.cachedHeaderBlob != nil {
			if err := jsoniter.Unmarshal(data.cachedHeaderBlob, &data.cachedHeader); err != nil {
				return err
			}
		}
		if data.bodyStructureBlob != nil {
			// don't reuse structure already sent to the channel
			data.bodyStructure = nil

			if err := jsoniter.Unmarshal(data.bodyStructureBlob, &data.bodyStructure); err != nil {
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
		var ent *message.Entity
		body, err := m.openBody(data.extBodyKey, data.headerBlob, data.bodyBlob)
		if err != nil {
			return err
		}
		bufferedBody := bufio.NewReader(body)

		if data.parsedHeader == nil {
			hdr, err := textproto.ReadHeader(bufferedBody)
			if err != nil {
				return err
			}
			data.parsedHeader = &message.Header{Header: hdr}
		}
		ent, err = message.New(*data.parsedHeader, bufferedBody)
		if err != nil {
			return err
		}

		msg.Body[sect], err = backendutil.FetchBodySection(ent, sect)
		if err != nil {
			msg.Body[sect] = bytes.NewReader(nil)
		}
	}

	return nil
}

func (m *Mailbox) openBody(extBodyKey sql.NullString, headerBlob, bodyBlob []byte) (io.ReadCloser, error) {
	if extBodyKey.Valid {
		rdr, err := m.parent.Opts.ExternalStore.Open(extBodyKey.String)
		if err != nil {
			return nil, err
		}
		return rdr, nil
	}
	return ioutil.NopCloser(io.MultiReader(bytes.NewReader(headerBlob), bytes.NewReader(bodyBlob))), nil
}

func headerSubsetFromCached(sect *imap.BodySectionName, cachedHeader map[string][]string) (imap.Literal, error) {
	hdr := message.Header{}
	for i := len(sect.Fields) - 1; i >= 0; i-- {
		field := sect.Fields[i]

		value := cachedHeader[strings.ToLower(field)]
		for i := len(value) - 1; i >= 0; i-- {
			subval := value[i]
			hdr.Add(field, subval)
		}
	}

	buf := new(bytes.Buffer)
	w, err := message.CreateWriter(buf, hdr)
	if err != nil {
		return nil, err
	}
	w.Close()

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
