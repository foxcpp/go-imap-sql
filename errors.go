package imapsql

import "fmt"

func wrapErr(err error, desc string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(desc+": %w", err)
}

func wrapErrf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	args = append(args, err)
	return fmt.Errorf(format+": %w", args...)
}
