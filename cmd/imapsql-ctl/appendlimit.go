package main

// Copied from go-imap-backend-tests.

// AppendLimitUser is extension for backend.User interface which allows to
// set append limit value for testing and administration purposes.
type AppendLimitUser interface {
	CreateMessageLimit() *uint32

	// SetMessageLimit sets new value for limit.
	// nil pointer means no limit.
	SetMessageLimit(val *uint32) error
}

// AppendLimitMbox is extension for backend.Mailbox interface which allows to
// set append limit value for testing and administration purposes.
type AppendLimitMbox interface {
	CreateMessageLimit() *uint32

	// SetMessageLimit sets new value for limit.
	// nil pointer means no limit.
	SetMessageLimit(val *uint32) error
}
