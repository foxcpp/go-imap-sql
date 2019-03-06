go-sqlmail
==========

[![Travis CI](https://img.shields.io/travis/com/foxcpp/go-sqlmail.svg?style=flat-square&logo=Linux)](https://travis-ci.com/foxcpp/go-sqlmail)
[![CodeCov](https://img.shields.io/codecov/c/github/foxcpp/go-sqlmail.svg?style=flat-square)](https://codecov.io/gh/foxcpp/go-sqlmail)
[![Issues](https://img.shields.io/github/issues-raw/foxcpp/go-sqlmail.svg?style=flat-square)](https://github.com/foxcpp/go-sqlmail/issues)
[![License](https://img.shields.io/github/license/foxcpp/go-sqlmail.svg?style=flat-square)](https://github.com/foxcpp/go-sqlmail/blob/master/LICENSE)

SQL-based storage backend for [go-imap] ~~and [go-smtp]~~ _(not yet)_ libraries.

#### Building

Go 1.11 is required because we are using modules. Things may work on older versions
but these configurations will not be supported.

#### RDBMS support

go-sqlmail is known to work with (and constantly being tested against) following RDBMS:
- SQLite 3.25.0
- MySQL 5.7 (or MariaDB 10.2)
- PostgreSQL 9.6

**Note:** MySQL 5.7 support is deprecated since it's addition and not even
strictly safe to use. Please stick to using newer versions (MySQL 8 or compatible MariaDB version)
when possible.

#### IMAP Extensions Supported

Due to go-imap architecture, some extensions require support from used backend.
Here are extensions supported by go-sqlmail:
- [CHILDREN]
- ~~[APPEND-LIMIT]~~ _(planned)_
- ~~[UIDPLUS]~~ _(planned)_
- [MOVE]
- ~~[IDLE]~~ _(planned)_

#### Maddy

You can try go-sqlmail as part of [maddy] mail server.  Currently it is not
merged into upstream yet so here is where you should get code from:
https://github.com/foxcpp/maddy/tree/sqlmail

You need to execute this command prior to building to get lastest development version:
```
go get github.com/foxcpp/go-sqlmail@dev
```

Here is minimal example for testing, using SQLite (you need CGo for SQLite!):
```
imap://127.0.0.1:1993 {
    sql sqlite3 maddy.db
    insecureauth
}
```

[CHILDREN]: https://tools.ietf.org/html/rfc3348
[APPEND-LIMIT]: https://tools.ietf.org/html/rfc7889
[UIDPLUS]: https://tools.ietf.org/html/rfc4315
[MOVE]: https://tools.ietf.org/html/rfc6851
[IDLE]: https://tools.ietf.org/html/rfc2177
[go-imap]: https://github.com/emersion/go-imap
[go-smtp]: https://github.com/emersion/go-smtp
[maddy]: https://github.com/emersion/maddy
