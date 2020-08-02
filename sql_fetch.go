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
	"From":         {},
	"To":           {},
	"Cc":           {},
	"Bcc":          {},
	"Subject":      {},
	"Date":         {},
	"Message-Id":   {},
	"Priority":     {},
	"x-Priority":   {},
	"References":   {},
	"Newsgroups":   {},
	"In-Reply-To":  {},
	"Content-Type": {},
	"Reply-To":     {},
	"Importance":   {},
	"List-Post":    {},

	// Requested by Apple Mail
	"X-Uniform-Type-Identifier":       {},
	"X-Universally-Unique-Identifier": {},

	// Misc fields I think clients could be interested in.
	"Sender":       {},
	"Return-Path":  {},
	"Delivered-To": {},
}

func (b *Backend) buildFetchStmt(items []imap.FetchItem) (stmt, cacheKey string, err error) {
	colNames := make(map[string]struct{}, len(items)+1)
	needFlags := false

	colNames["msgs.msgId"] = struct{}{}

	for _, item := range items {
		switch item {
		case imap.FetchInternalDate:
			colNames["date"] = struct{}{}
		case imap.FetchRFC822Size:
			colNames["bodyLen"] = struct{}{}
		case imap.FetchUid:
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

	columns := strings.Join(cols, ", ")
	return `SELECT ` + columns + `
		FROM msgs
		` + extraParams + `
		WHERE msgs.mboxId = ? AND msgs.msgId BETWEEN ? AND ?
		GROUP BY msgs.mboxId, msgs.msgId`, columns, nil
}

func (b *Backend) getFetchStmt(items []imap.FetchItem) (*sql.Stmt, error) {
	str, key, err := b.buildFetchStmt(items)
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
