package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"

	appendlimit "github.com/emersion/go-imap-appendlimit"
	move "github.com/emersion/go-imap-move"
	sortthread "github.com/emersion/go-imap-sortthread"
	specialuse "github.com/emersion/go-imap-specialuse"
	"github.com/emersion/go-imap/server"
	imapsql "github.com/foxcpp/go-imap-sql"
	"github.com/foxcpp/go-imap-sql/children"
)

type stdLogger struct{}

func (s stdLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (s stdLogger) Println(v ...interface{}) {
	log.Println(v...)
}

func (s stdLogger) Debugf(format string, v ...interface{}) {
	log.Printf("debug: "+format, v...)
}

func (s stdLogger) Debugln(v ...interface{}) {
	v = append([]interface{}{"debug:"}, v...)
	log.Println(v...)
}

func main() {
	if len(os.Args) < 5 {
		fmt.Fprintf(os.Stderr, "imapd - Dumb IMAP4rev1 server providing unauthenticated access a go-imap-sql db\n")
		fmt.Fprintf(os.Stderr, "Usage: %s <endpoint> <driver> <dsn> <fsstore>\n", os.Args[0])
		os.Exit(2)
	}

	runtime.SetCPUProfileRate(200)
	go http.ListenAndServe("127.0.0.2:9999", nil)

	endpoint := os.Args[1]
	driver := os.Args[2]
	dsn := os.Args[3]
	fsStore := imapsql.FSStore{Root: os.Args[4]}

	bkd, err := imapsql.New(driver, dsn, &fsStore, imapsql.Opts{
		BusyTimeout: 100000,
		Log:         stdLogger{},
	})
	defer bkd.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Backend initialization failed: %v\n", err)
		os.Exit(2)
	}

	srv := server.New(bkd)
	defer srv.Close()

	srv.AllowInsecureAuth = true
	srv.Enable(move.NewExtension())
	srv.Enable(children.NewExtension())
	srv.Enable(specialuse.NewExtension())
	srv.Enable(sortthread.NewSortExtension())
	srv.Enable(sortthread.NewThreadExtension())
	srv.Enable(appendlimit.NewExtension())

	l, err := net.Listen("tcp", endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	go func() {
		if err := srv.Serve(l); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	<-sig
}
