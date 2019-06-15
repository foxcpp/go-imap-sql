package imapsql

import (
	"testing"

	"gotest.tools/assert"
)

func TestInboxCreation(t *testing.T) {
	b := initTestBackend().(*Backend)

	assert.NilError(t, b.CreateUser("foxcpp", ""))

	u, err := b.GetUser("foxcpp")
	assert.NilError(t, err, "b.GetUser")

	_, err = u.GetMailbox("INBOX")
	assert.NilError(t, err, "u.GetMailbox")
}
