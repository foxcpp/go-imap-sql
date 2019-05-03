package imapsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"net/textproto"
	"strings"
	"time"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
	jsoniter "github.com/json-iterator/go"
)

func (m *Mailbox) ListMessages(uid bool, seqset *imap.SeqSet, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer close(ch)

	stmt, err := m.parent.getFetchStmt(uid, items)
	if err != nil {
		return err
	}
	// don't close statement, it is owned by cache

	for _, seq := range seqset.Set {
		begin, end := sqlRange(seq)

		rows, err := stmt.Query(m.id, m.id, begin, end)
		if err != nil {
			return err
		}
		if err := scanMessages(rows, items, ch); err != nil {
			return err
		}
	}

	return nil
}

func scanMessages(rows *sql.Rows, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer rows.Close()

	var cachedHeaderBlob, bodyStructureBlob, headerBlob, bodyBlob []byte
	var seqNum, msgId uint32
	var dateUnix int64
	var headerLen, bodyLen uint32
	var flagStr string

	var bodyStructure *imap.BodyStructure
	var cachedHeader map[string][]string

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	scanOrder := make([]interface{}, 0, len(cols))
	for _, col := range cols {
		switch col {
		case "seqnum":
			scanOrder = append(scanOrder, &seqNum)
		case "date":
			scanOrder = append(scanOrder, &dateUnix)
		case "headerLen":
			scanOrder = append(scanOrder, &headerLen)
		case "bodyLen":
			scanOrder = append(scanOrder, &bodyLen)
		case "msgId":
			scanOrder = append(scanOrder, &msgId)
		case "cachedHeader":
			scanOrder = append(scanOrder, &cachedHeaderBlob)
		case "bodyStructure":
			scanOrder = append(scanOrder, &bodyStructureBlob)
		case "header":
			scanOrder = append(scanOrder, &headerBlob)
		case "body":
			scanOrder = append(scanOrder, &bodyBlob)
		case "flags":
			scanOrder = append(scanOrder, &flagStr)
		default:
			panic("unknown column: " + col)
		}
	}

	for rows.Next() {
		if err := rows.Scan(scanOrder...); err != nil {
			return err
		}

		if cachedHeaderBlob != nil {
			if err := jsoniter.Unmarshal(cachedHeaderBlob, &cachedHeader); err != nil {
				return err
			}
		}
		if bodyStructureBlob != nil {
			// don't reuse structure already sent to the channel
			bodyStructure = nil

			if err := jsoniter.Unmarshal(bodyStructureBlob, &bodyStructure); err != nil {
				return err
			}
		}

		var parsedHdr message.Header
		msg := imap.NewMessage(seqNum, items)

		for _, item := range items {
			switch item {
			case imap.FetchInternalDate:
				msg.InternalDate = time.Unix(dateUnix, 0)
			case imap.FetchRFC822Size:
				msg.Size = headerLen + bodyLen
			case imap.FetchUid:
				msg.Uid = msgId
			case imap.FetchEnvelope:
				raw := envelopeFromHeader(cachedHeader)
				msg.Envelope = raw.toIMAP()
			case imap.FetchBody:
				msg.BodyStructure = stripExtBodyStruct(bodyStructure)
			case imap.FetchBodyStructure:
				msg.BodyStructure = bodyStructure
			case imap.FetchFlags:
				msg.Flags = strings.Split(flagStr, flagsSep)
			default:
				sect, part, err := getNeededPart(item)
				if err != nil {
					return err
				}

				switch part {
				case needCachedHeader:
					var err error
					msg.Body[sect], err = headerSubsetFromCached(sect, cachedHeader)
					if err != nil {
						return err
					}
				case needHeader, needFullBody:
					if parsedHdr == nil {
						textprotoHdr, err := textproto.NewReader(bufio.NewReader(bytes.NewReader(headerBlob))).ReadMIMEHeader()
						if err != nil {
							return err
						}
						parsedHdr = message.Header(textprotoHdr)
					}

					ent, err := message.New(parsedHdr, bytes.NewReader(bodyBlob))
					if err != nil {
						return err
					}

					msg.Body[sect], err = backendutil.FetchBodySection(ent, sect)
					if err != nil {
						msg.Body[sect] = bytes.NewReader(nil)
					}
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

func headerSubsetFromCached(sect *imap.BodySectionName, cachedHeader map[string][]string) (imap.Literal, error) {
	hdr := make(message.Header, len(sect.Fields))
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
