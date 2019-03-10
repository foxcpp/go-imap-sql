package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"

	"github.com/foxcpp/go-sqlmail"
	"github.com/foxcpp/go-sqlmail/imap"
	"github.com/urfave/cli"
)

var backend *imap.Backend
var stdinScnr *bufio.Scanner

func connectToDB(ctx *cli.Context) (err error) {
	if ctx.IsSet("unsafe") && !ctx.IsSet("quiet") {
		fmt.Fprintln(os.Stderr, "WARNING: Using --unsafe with running server may lead to accidential damage to data due to desynchronization with connected clients.")
	}

	driver := ctx.String("driver")
	dsn := ctx.String("dsn")

	if (driver == "" || dsn == "") && ctx.IsSet("config") {
		f, err := os.Open(ctx.String("config"))
		if err != nil {
			return err
		}
		defer f.Close()
		scnr := bufio.NewScanner(f)

		if !scnr.Scan() {
			return scnr.Err()
		}
		driver = scnr.Text()

		if !scnr.Scan() {
			return scnr.Err()
		}
		dsn = scnr.Text()
	}

	if driver == "" {
		return errors.New("Error: driver is required")
	}
	if dsn == "" {
		return errors.New("Error: dsn is required")
	}

	backend, err = imap.NewBackend(driver, dsn, imap.Opts{})
	return
}

func closeBackend(ctx *cli.Context) (err error) {
	if backend != nil {
		return backend.Close()
	}
	return nil
}

func main() {
	stdinScnr = bufio.NewScanner(os.Stdin)

	app := cli.NewApp()
	app.Usage = "go-sqlmail database management utility"
	app.Version = sqlmail.VersionStr + " (go-sqlmail)"
	app.HideVersion = true
	app.Before = connectToDB
	app.After = closeBackend

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "driver",
			Usage: "SQL driver to use for communication with DB",
		},
		cli.StringFlag{
			Name:  "dsn",
			Usage: "Data Source Name to use\n\t\tWARNING: Provided only for debugging convenience. Don't leave your passwords in shell history!",
		},
		cli.StringFlag{
			Name:   "config,c",
			Usage:  "Read driver and DSN values from file",
			EnvVar: "SQLMAIL_CREDS",
		},
		cli.BoolFlag{
			Name:  "quiet,q",
			Usage: "Don't print user-friendly messages to stderr",
		},
		cli.BoolFlag{
			Name:  "unsafe",
			Usage: "Allow to perform actions that can be safely done only without running server",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "mboxes",
			Usage: "Mailboxes (folders) management",
			Subcommands: []cli.Command{
				{
					Name:      "list",
					Usage:     "Show mailboxes of user",
					ArgsUsage: "USERNAME",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "subscribed,s",
							Usage: "List only subscribed mailboxes",
						},
					},
					Action: mboxesList,
				},
				{
					Name:      "create",
					Usage:     "Create mailbox",
					ArgsUsage: "USERNAME NAME",
					Action:    mboxesCreate,
				},
				{
					Name:        "remove",
					Usage:       "Remove mailbox (requires --unsafe)",
					Description: "WARNING: All contents of mailbox will be irrecoverably lost.",
					ArgsUsage:   "USERNAME MAILBOX",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "yes,y",
							Usage: "Don't ask for confirmation",
						},
					},
					Action: mboxesRemove,
				},
				{
					Name:        "rename",
					Usage:       "Rename mailbox (requires --unsafe)",
					Description: "Rename may cause unexpected failures on client-side so be careful.",
					ArgsUsage:   "USERNAME OLDNAME NEWNAME",
				},
				{
					Name:      "appendlimit",
					Usage:     "Query or set user's APPENDLIMIT value",
					ArgsUsage: "USERNAME",
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "value,v",
							Usage: "Set APPENDLIMIT to specified value (in bytes). Pass -1 to disable limit.",
						},
					},
					Action: mboxesAppendLimit,
				},
			},
		},
		{
			Name:  "msgs",
			Usage: "Messages management",
			Subcommands: []cli.Command{
				{
					Name:        "add",
					Usage:       "Add message to mailbox (requires --unsafe)",
					ArgsUsage:   "USERNAME MAILBOX",
					Description: "Reads message body (with headers) from stdin. Prints UID of created message on success.",
					Flags: []cli.Flag{
						cli.StringSliceFlag{
							Name:  "flag,f",
							Usage: "Add flag to message. Can be specified multiple times",
						},
						cli.Int64Flag{
							Name:  "date,d",
							Usage: "Set internal date value to specified UNIX timestamp",
						},
					},
					Action: msgsAdd,
				},
				{
					Name:      "remove",
					Usage:     "Remove messages from mailbox (requires --unsafe)",
					ArgsUsage: "USERNAME MAILBOX SEQSET",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "uid,u",
							Usage: "Use UIDs for SEQSET instead of sequence numbers",
						},
						cli.BoolFlag{
							Name:  "yes,y",
							Usage: "Don't ask for confirmation",
						},
					},
					Action: msgsRemove,
				},
				{
					Name:        "copy",
					Usage:       "Copy messages between mailboxes (requires --unsafe)",
					Description: "Note: You can't copy between mailboxes of different users. APPENDLIMIT of target mailbox is not enforced.",
					ArgsUsage:   "USERNAME SRCMAILBOX SEQSET TGTMAILBOX",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "uid,u",
							Usage: "Use UIDs for SEQSET instead of sequence numbers",
						},
					},
					Action: msgsCopy,
				},
				{
					Name:        "move",
					Usage:       "Move messages between mailboxes (requires --unsafe)",
					Description: "Note: You can't move between mailboxes of different users. APPENDLIMIT of target mailbox is not enforced.",
					ArgsUsage:   "USERNAME SRCMAILBOX SEQSET TGTMAILBOX",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "uid,u",
							Usage: "Use UIDs for SEQSET instead of sequence numbers",
						},
					},
					Action: msgsMove,
				},
				{
					Name:        "list",
					Usage:       "List messages in mailbox",
					Description: "If SEQSET is specified - only show messages that match it.",
					ArgsUsage:   "USERNAME MAILBOX [SEQSET]",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "uid,u",
							Usage: "Use UIDs for SEQSET instead of sequence numbers",
						},
						cli.BoolFlag{
							Name:  "full,f",
							Usage: "Show entire envelope and all server meta-data",
						},
					},
					Action: msgsList,
				},
				{
					Name:        "dump",
					Usage:       "Dump message body",
					Description: "If passed SEQ matches multiple messages - they will be joined.",
					ArgsUsage:   "USERNAME MAILBOX SEQ",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "uid,u",
							Usage: "Use UIDs for SEQ instead of sequence numbers",
						},
					},
					Action: msgsDump,
				},
			},
		},
		{
			Name:  "users",
			Usage: "User accounts management",
			Subcommands: []cli.Command{
				{
					Name:   "list",
					Usage:  "List created user accounts",
					Action: usersList,
				},
				{
					Name:        "create",
					Usage:       "Create user account",
					Description: "Reads password from stdin",
					ArgsUsage:   "USERNAME",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "password,p",
							Usage: "Use `PASSWORD instead of reading password from stdin.\n\t\tWARNING: Provided only for debugging convenience. Don't leave your passwords in shell history!",
						},
					},
					Action: usersCreate,
				},
				{
					Name:      "remove",
					Usage:     "Delete user account (requires --unsafe)",
					ArgsUsage: "USERNAME",
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "yes,y",
							Usage: "Don't ask for confirmation",
						},
					},
					Action: usersRemove,
				},
				{
					Name:        "password",
					Usage:       "Change account password",
					Description: "Reads password from stdin",
					ArgsUsage:   "USERNAME",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "password,p",
							Usage: "Use `PASSWORD` instead of reading password from stdin.\n\t\tWARNING: Provided only for debugging convenience. Don't leave your passwords in shell history!",
						},
					},
					Action: usersPassword,
				},
				{
					Name:      "appendlimit",
					Usage:     "Query or set user's APPENDLIMIT value",
					ArgsUsage: "USERNAME",
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "value,v",
							Usage: "Set APPENDLIMIT to specified value (in bytes)",
						},
					},
					Action: usersAppendLimit,
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
