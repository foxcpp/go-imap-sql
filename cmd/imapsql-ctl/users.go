package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/urfave/cli"
)

func usersList(ctx *cli.Context) error {
	list, err := backend.ListUsers()
	if err != nil {
		return err
	}

	if len(list) == 0 && !ctx.GlobalBool("quiet") {
		fmt.Fprintln(os.Stderr, "No users.")
	}

	for _, user := range list {
		fmt.Println(user)
	}
	return nil
}

func usersCreate(ctx *cli.Context) error {
	username := ctx.Args().First()
	if username == "" {
		return errors.New("Error: USERNAME is required")
	}

	_, err := backend.GetExistingUser(username)
	if err == nil {
		return errors.New("Error: User already exists")
	}

	var pass string
	if ctx.IsSet("password") {
		pass = ctx.String("password,p")
	} else {
		pass, err = ReadPassword("Enter password for new user")
		if err != nil {
			return err
		}
	}

	return backend.CreateUser(username, pass)
}

func usersRemove(ctx *cli.Context) error {
	if !ctx.GlobalBool("unsafe") {
		return errors.New("Error: Refusing to edit mailboxes without --unsafe")
	}

	username := ctx.Args().First()
	if username == "" {
		return errors.New("Error: USERNAME is required")
	}

	_, err := backend.GetUser(username)
	if err != nil {
		return errors.New("Error: User doesn't exists")
	}

	if !ctx.Bool("yes") {
		if !Confirmation("Are you sure you want to delete this user account?", false) {
			return errors.New("Cancelled")
		}
	}

	return backend.DeleteUser(username)
}

func usersPassword(ctx *cli.Context) error {
	username := ctx.Args().First()
	if username == "" {
		return errors.New("Error: USERNAME is required")
	}

	_, err := backend.GetUser(username)
	if err != nil {
		return errors.New("Error: User doesn't exists")
	}

	var pass string
	if ctx.IsSet("password") {
		pass = ctx.String("password")
	} else {
		pass, err = ReadPassword("Enter new password")
		if err != nil {
			return err
		}
	}

	return backend.SetUserPassword(username, pass)
}

func usersAppendLimit(ctx *cli.Context) error {
	username := ctx.Args().First()
	if username == "" {
		return errors.New("Error: USERNAME is required")
	}

	u, err := backend.GetUser(username)
	if err != nil {
		return err
	}
	userAL := u.(AppendLimitUser)

	if ctx.IsSet("value") {
		val := ctx.Int("value")

		var err error
		if val == -1 {
			err = userAL.SetMessageLimit(nil)
		} else {
			val32 := uint32(val)
			err = userAL.SetMessageLimit(&val32)
		}
		if err != nil {
			return err
		}
	} else {
		lim := userAL.CreateMessageLimit()
		if lim == nil {
			fmt.Println("No limit")
		} else {
			fmt.Println(*lim)
		}
	}

	return nil
}
