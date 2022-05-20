package imapsql

import (
	"net/mail"
	"strings"
	"time"

	"errors"

	imap "github.com/emersion/go-imap"
)

type rawEnvelope struct {
	Date      int64
	Subject   string
	From      string
	Sender    string
	ReplyTo   string
	To        string
	CC        string
	BCC       string
	InReplyTo string
	MessageID string
}

func envelopeFromHeader(hdr map[string][]string) rawEnvelope {
	enve := rawEnvelope{}
	date := hdr["Date"]
	if date != nil {
		t, err := parseMessageDateTime(date[0])
		if err == nil {
			enve.Date = t.Unix()
		}
	}

	addrFields := [...]string{"From", "Sender", "Reply-To", "To", "Cc", "Bcc", "In-Reply-To"}
	for i, fieldVar := range [...]*string{
		&enve.From, &enve.Sender, &enve.ReplyTo,
		&enve.To, &enve.CC, &enve.BCC, &enve.InReplyTo,
	} {
		val := hdr[addrFields[i]]
		if val == nil {
			continue
		}

		*fieldVar = strings.Join(val, ", ")
	}

	if enve.Sender == "" {
		enve.Sender = enve.From
	}
	if enve.ReplyTo == "" {
		enve.ReplyTo = enve.From
	}

	fields := [...]string{"Subject", "Message-Id"}
	for i, fieldVar := range [...]*string{
		&enve.Subject, &enve.MessageID,
	} {
		val := hdr[fields[i]]
		if val == nil {
			continue
		}

		*fieldVar = val[0]
	}
	return enve
}

func toImapAddr(list []*mail.Address) ([]*imap.Address, error) {
	res := make([]*imap.Address, 0, len(list))
	for _, mailAddr := range list {
		imapAddr := imap.Address{}
		imapAddr.PersonalName = mailAddr.Name
		addrParts := strings.Split(mailAddr.Address, "@")
		if len(addrParts) != 2 {
			return res, errors.New("imap: malformed address")
		}

		imapAddr.MailboxName = addrParts[0]
		imapAddr.HostName = addrParts[1]
		res = append(res, &imapAddr)
	}
	return res, nil
}

func (enve *rawEnvelope) toIMAP() *imap.Envelope {
	res := new(imap.Envelope)
	res.Date = time.Unix(enve.Date, 0)
	res.Subject = enve.Subject
	from, _ := mail.ParseAddressList(enve.From)
	res.From, _ = toImapAddr(from)
	// I really wonder how we can have multiple senders in a message header,
	// but imap.Envelope says we can.
	sender, _ := mail.ParseAddressList(enve.Sender)
	res.Sender, _ = toImapAddr(sender)
	replyTo, _ := mail.ParseAddressList(enve.ReplyTo)
	res.ReplyTo, _ = toImapAddr(replyTo)
	to, _ := mail.ParseAddressList(enve.To)
	res.To, _ = toImapAddr(to)
	cc, _ := mail.ParseAddressList(enve.CC)
	res.Cc, _ = toImapAddr(cc)
	bcc, _ := mail.ParseAddressList(enve.BCC)
	res.Bcc, _ = toImapAddr(bcc)
	res.InReplyTo = enve.InReplyTo
	res.MessageId = enve.MessageID
	return res
}
