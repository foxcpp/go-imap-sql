module github.com/foxcpp/go-imap-sql

go 1.12

require (
	github.com/emersion/go-imap v1.0.5-0.20200511082158-271ea913b422
	github.com/emersion/go-imap-appendlimit v0.0.0-20190308131241-25671c986a6a
	github.com/emersion/go-imap-move v0.0.0-20180601155324-5eb20cb834bf
	github.com/emersion/go-imap-sortthread v1.1.1-0.20200727121200-18e5fb409fed
	github.com/emersion/go-imap-specialuse v0.0.0-20161227184202-ba031ced6a62
	github.com/emersion/go-message v0.11.2
	github.com/foxcpp/go-imap-backend-tests v0.0.0-20200617132817-958ea5829771
	github.com/foxcpp/go-imap-namespace v0.0.0-20200722130255-93092adf35f1
	github.com/foxcpp/go-imap-sequpdate v0.0.0-20190308131241-25671c986a6a
	github.com/frankban/quicktest v1.5.0 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/klauspost/compress v1.10.5
	github.com/lib/pq v1.4.0
	github.com/mailru/easyjson v0.7.1
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/pierrec/lz4 v2.5.2+incompatible
	github.com/urfave/cli v1.20.0
	google.golang.org/appengine v1.6.1 // indirect
	gotest.tools v2.2.0+incompatible
)

replace github.com/emersion/go-imap => ../go-imap

replace github.com/foxcpp/go-imap-sequpdate => ../go-imap-sequpdate

replace github.com/foxcpp/go-imap-backend-tests => ../go-imap-backend-tests

replace github.com/foxcpp/go-imap-namespace => ../go-imap-namespace
