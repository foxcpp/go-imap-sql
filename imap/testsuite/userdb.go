package testsuite

import (
	"testing"

	"github.com/emersion/go-imap/backend"
	"github.com/foxcpp/go-sqlmail"
	"gotest.tools/assert"
)

func UserDB_CreateUser(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)

	assert.NilError(t, b.CreateUser("username1", "password1"), "CreateUser username1 failed")
	assert.Error(t, b.CreateUser("username1", "password1"), sqlmail.ErrUserAlreadyExists.Error(), "CreateUser username1 (again) failed")

	u, err := b.GetUser("username1")
	assert.NilError(t, err, "GetUser username1 failed")
	assert.NilError(t, u.Logout())

	_, err = b.GetUser("username2")
	assert.Error(t, err, sqlmail.ErrUserDoesntExists.Error(), "GetUser username2 failed")
}

func UserDB_Login(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)

	assert.NilError(t, b.CreateUser("username1", "password1"), "CreateUser username1 failed")

	u, err := b.Login("username1", "password1")
	assert.NilError(t, err, "Login username1")
	assert.NilError(t, u.Logout())
}

func UserDB_SetPassword(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)

	assert.NilError(t, b.CreateUser("username1", "password1"), "CreateUser username1 failed")

	u, err := b.Login("username1", "password1")
	assert.NilError(t, err, "Login with original password")
	assert.NilError(t, u.Logout())

	assert.NilError(t, b.SetUserPassword("username1", "password2"), "SetPassword failed")

	_, err = b.Login("username1", "password1")
	assert.Error(t, err, backend.ErrInvalidCredentials.Error(), "Login with original password (again) failed")

	u, err = b.Login("username1", "password2")
	assert.NilError(t, err, "Login with new password failed")
	assert.NilError(t, u.Logout())
}

func UserDB_DeleteUser(t *testing.T, newBack newBackFunc, closeBack closeBackFunc) {
	b := newBack()
	defer closeBack(b)

	assert.Error(t, b.DeleteUser("username1"), sqlmail.ErrUserDoesntExists.Error(), "DeleteUser username1 (non existent) failed")
	assert.NilError(t, b.CreateUser("username1", "password1"), "CreateUser username1 failed")
	assert.NilError(t, b.DeleteUser("username1"), "DeleteUser username1 failed")
	_, err := b.Login("username1", "password1")
	assert.Error(t, err, backend.ErrInvalidCredentials.Error(), "Login username failed")
}
