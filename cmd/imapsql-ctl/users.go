package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/urfave/cli"
)

func usersList(ctx *cli.Context) error {
	if err := connectToDB(ctx); err != nil {
		return err
	}

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
	if err := connectToDB(ctx); err != nil {
		return err
	}

	username := ctx.Args().First()
	if username == "" {
		return errors.New("Error: USERNAME is required")
	}

	_, err := backend.GetUser(username)
	if err == nil {
		return errors.New("Error: User already exists")
	}

	return backend.CreateUser(username)
}

func usersRemove(ctx *cli.Context) error {
	if err := connectToDB(ctx); err != nil {
		return err
	}

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

func usersAppendLimit(ctx *cli.Context) error {
	if err := connectToDB(ctx); err != nil {
		return err
	}

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
