package imapsql

import (
	"testing"

	"gotest.tools/assert"
)

func TestUserCaseInsensitivity(t *testing.T) {
	b := initTestBackend().(*Backend)
	b.Opts.DefaultHashAlgo = "sha3-512"

	assert.NilError(t, b.CreateUser("foXcpp", ""))
	assert.ErrorContains(t, b.CreateUserWithHash("foXCPP", "sha3-512", ""), ErrUserAlreadyExists.Error())
	assert.Assert(t, b.CheckPlain("FOXCPP", ""))
	assert.NilError(t, b.ResetPassword("foxCPP"))
	assert.Assert(t, !b.CheckPlain("foXcPp", ""))
	assert.NilError(t, b.SetUserPassword("FOXCPp", ""))
	_, err := b.Login(nil, "foXCpp", "")
	assert.NilError(t, err, "b.Login")
	u1, err := b.GetUser("Foxcpp")
	assert.NilError(t, err, "b.GetUser")
	u2, err := b.GetOrCreateUser("FOXcpp")
	assert.NilError(t, err, "b.GetOrCreateUser")

	assert.NilError(t, u1.CreateMailbox("BOX"))
	_, err = u2.GetMailbox("BOX")
	assert.NilError(t, err, "u2.GetMailbox")
}
