imapsql-ctl utility
-------------------

Low-level tool for go-imap-sql database management. Minimal wrapper for Backend methods.

#### --help

```
NAME:
   imapsql-ctl users - User accounts management

USAGE:
   imapsql-ctl users [global options] command [command options] [arguments...]

COMMANDS:
     list         List created user accounts
     create       Create user account
     remove       Delete user account (requires --unsafe)
     password     Change account password
     appendlimit  Query or set user's APPENDLIMIT value

GLOBAL OPTIONS:
   --help, -h  show help


NAME:
   imapsql-ctl mboxes - Mailboxes (folders) management

USAGE:
   imapsql-ctl mboxes [global options] command [command options] [arguments...]

COMMANDS:
     list         Show mailboxes of user
     create       Create mailbox
     remove       Remove mailbox (requires --unsafe)
     rename       Rename mailbox (requires --unsafe)
     appendlimit  Query or set user's APPENDLIMIT value

GLOBAL OPTIONS:
   --help, -h  show help


NAME:
   imapsql-ctl users - User accounts management

USAGE:
   imapsql-ctl users [global options] command [command options] [arguments...]

COMMANDS:
     list         List created user accounts
     create       Create user account
     remove       Delete user account (requires --unsafe)
     password     Change account password
     appendlimit  Query or set user's APPENDLIMIT value

GLOBAL OPTIONS:
   --help, -h  show help


NAME:
   imapsql-ctl - go-imap-sql database management utility

USAGE:
   imapsql-ctl [global options] command [command options] [arguments...]

COMMANDS:
     mboxes   Mailboxes (folders) management
     msgs     Messages management
     users    User accounts management
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --driver value            SQL driver to use for communication with DB
   --dsn value               Data Source Name to use
                               WARNING: Provided only for debugging convenience. Don't leave your passwords in shell history!
   --config value, -c value  Read driver and DSN values from file [$SQLMAIL_CREDS]
   --quiet, -q               Don't print user-friendly messages to stderr
   --unsafe                  Allow to perform actions that can be safely done only without running server
   --help, -h                show help
```

#### --unsafe option

Per RFC 3501, server must send notifications to clients about any mailboxes
change. Since imapsql-ctl is a low-level tool it doesn't implements any way to
tell server to send such notifications. Most popular SQL RDBMSs don't provide
any means to detect database change and we currently have no plans on
implementing anything for that on go-imap-sql level.

Therefore, you generally should avoid writting to mailboxes if client who owns
this mailbox is connected to the server. Failure to send required notifications
may result in data damage depending on client implementation.
