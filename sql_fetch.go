package imapsql

import (
	"database/sql"
	nettextproto "net/textproto"
	"sort"
	"strings"

	"github.com/emersion/go-imap"
)

const flagsMidBlock = `
	LEFT JOIN flags
	ON flags.msgId = msgs.msgId AND msgs.mboxId = flags.mboxId`

var cachedHeaderFields = map[string]struct{}{
	// Common header fields (requested by Thunderbird)
	"From":         struct{}{},
	"To":           struct{}{},
	"Cc":           struct{}{},
	"Bcc":          struct{}{},
	"Subject":      struct{}{},
	"Date":         struct{}{},
	"Message-Id":   struct{}{},
	"Priority":     struct{}{},
	"x-Priority":   struct{}{},
	"References":   struct{}{},
	"Newsgroups":   struct{}{},
	"In-Reply-To":  struct{}{},
	"Content-Type": struct{}{},
	"Reply-To":     struct{}{},
	"Importance":   struct{}{},
	"List-Post":    struct{}{},

	// Requested by Apple Mail
	"X-Uniform-Type-Identifier":       struct{}{},
	"X-Universally-Unique-Identifier": struct{}{},

	// Misc fields I think clients could be interested in.
	"Return-Path":  struct{}{},
	"Delivered-To": struct{}{},
}

func (b *Backend) buildFetchStmt(uid bool, items []imap.FetchItem) (stmt, cacheKey string, err error) {
	colNames := make(map[string]struct{}, len(items))
	needFlags := false

	for _, item := range items {
		switch item {
		case imap.FetchInternalDate:
			colNames["date"] = struct{}{}
		case imap.FetchRFC822Size:
			colNames["bodyLen"] = struct{}{}
		case imap.FetchUid:
			colNames["msgs.msgId"] = struct{}{}
		case imap.FetchEnvelope:
			colNames["cachedHeader"] = struct{}{}
		case imap.FetchFlags:
			needFlags = true
		case imap.FetchBody, imap.FetchBodyStructure:
			colNames["bodyStructure"] = struct{}{}
		default:
			_, part, err := getNeededPart(item)
			if err != nil {
				return "", "", err
			}

			switch part {
			case needCachedHeader:
				colNames["cachedHeader"] = struct{}{}
			case needHeader, needFullBody:
				colNames["extBodyKey"] = struct{}{}
				colNames["compressAlgo"] = struct{}{}
			}
		}
	}

	cols := make([]string, 0, len(colNames)+1)
	for col := range colNames {
		cols = append(cols, col)
	}
	extraParams := ""
	if needFlags {
		extraParams = flagsMidBlock
		cols = append(cols, b.db.aggrValuesSet("flag", "{")+" AS flags")
	}

	sort.Strings(cols)

	filterId := "seqnum"
	if uid {
		filterId = "msgs.msgId"
	}

	columns := strings.Join(cols, ", ")
	return `SELECT seqnum, ` + columns + `
		FROM msgs
		INNER JOIN (
			SELECT row_number() OVER (ORDER BY msgId) AS seqnum, msgId, mboxId
			FROM msgs
			WHERE mboxId = ?
		) map
		ON map.msgId = msgs.msgId
		` + extraParams + `
		WHERE msgs.mboxId = ? AND ` + filterId + ` BETWEEN ? AND ?
		GROUP BY seqnum, msgs.mboxId, msgs.msgId`, filterId + "/" + columns, nil
}

func (b *Backend) getFetchStmt(uid bool, items []imap.FetchItem) (*sql.Stmt, error) {
	str, key, err := b.buildFetchStmt(uid, items)
	if err != nil {
		return nil, err
	}

	b.fetchStmtsLck.RLock()
	stmt := b.fetchStmtsCache[key]
	b.fetchStmtsLck.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	stmt, err = b.db.Prepare(str)
	if err != nil {
		return nil, err
	}

	b.fetchStmtsLck.Lock()
	b.fetchStmtsCache[key] = stmt
	b.fetchStmtsLck.Unlock()
	return stmt, nil
}

type neededPart int

const (
	needCachedHeader neededPart = iota
	needHeader
	needFullBody
)

func getNeededPart(item imap.FetchItem) (*imap.BodySectionName, neededPart, error) {
	var sect *imap.BodySectionName
	sect, err := imap.ParseBodySectionName(item)
	if err != nil {
		return nil, -1, err
	}

	onlyHeader := false
	onlyCached := false
	switch sect.Specifier {
	case imap.MIMESpecifier, imap.HeaderSpecifier:
		onlyHeader = len(sect.Path) == 0
		if sect.Fields != nil && !sect.NotFields && onlyHeader {
			onlyCached = true
			for _, field := range sect.Fields {
				cKey := nettextproto.CanonicalMIMEHeaderKey(field)
				if _, ok := cachedHeaderFields[cKey]; !ok {
					onlyCached = false
				}
			}
		}
	}

	if onlyCached && onlyHeader {
		return sect, needCachedHeader, nil
	}
	if !onlyHeader {
		return sect, needFullBody, nil
	}
	return sect, needHeader, nil
}
