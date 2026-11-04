package imapsql

import (
	"log"
	"strconv"
)

type globalLogger struct{}

func (globalLogger) Printf(format string, v ...any) {
	log.Printf(format, v...)
}

func (globalLogger) Println(v ...any) {
	log.Println(v...)
}

func (globalLogger) Debugf(format string, v ...any) {
	log.Printf(format, v...)
}

func (globalLogger) Debugln(v ...any) {
	log.Println(v...)
}

type DummyLogger struct{}

func (DummyLogger) Printf(format string, v ...any) {}
func (DummyLogger) Println(v ...any)               {}
func (DummyLogger) Debugf(format string, v ...any) {}
func (DummyLogger) Debugln(v ...any)               {}

func (b *Backend) logUserErr(u *User, err error, when string, args ...any) {
	if err == nil {
		return
	}
	b.Opts.Log.Printf("%s %v: %v \t{\"username\":%s,\"uid\":%d}",
		when, args, err, strconv.Quote(u.username), u.id)
}

func (b *Backend) logMboxErr(m *Mailbox, err error, when string, args ...any) {
	if err == nil {
		return
	}
	b.Opts.Log.Printf("%s %v: %v \t{\"mbox\":%s,\"mboxId\":%d,\"username\":%s,\"uid\":%d}",
		when, args, err, strconv.Quote(m.name), m.id, strconv.Quote(m.user.username), m.user.id)
}
