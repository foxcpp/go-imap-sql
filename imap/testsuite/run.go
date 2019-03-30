package testsuite

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/emersion/go-imap/backend"
	"github.com/foxcpp/go-imap-sql"
)

type Backend interface {
	backend.Backend
	imapsql.IMAPUsersDB
}

type NewBackFunc func() Backend
type CloseBackFunc func(Backend)

type testFunc func(*testing.T, NewBackFunc, CloseBackFunc)

func GetFunctionName(i interface{}) string {
	parts := strings.Split(runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name(), "/")
	prefix := "testsuite."
	return parts[len(parts)-1][len(prefix):]
}

func RunTests(t *testing.T, newBackend NewBackFunc, closeBackend CloseBackFunc) {
	addTest := func(f testFunc) {
		t.Run(GetFunctionName(f), func(t *testing.T) {
			f(t, newBackend, closeBackend)
		})
	}

	addTest(TestInit)
	addTest(UserDB_CreateUser)
	addTest(UserDB_Login)
	addTest(UserDB_DeleteUser)
	addTest(UserDB_SetPassword)
	addTest(User_Username)
	addTest(User_ListMailboxes)
	addTest(User_CreateMailbox)
	addTest(User_CreateMailbox_Parents)
	addTest(User_DeleteMailbox)
	addTest(User_DeleteMailbox_Parents)
	addTest(User_RenameMailbox)
	addTest(User_RenameMailbox_Childrens)
	addTest(User_RenameMailbox_INBOX)
	addTest(Mailbox_Info)
	addTest(Mailbox_Status)
	addTest(Mailbox_SetSubscribed)
	addTest(Mailbox_CreateMessage)
	addTest(Mailbox_UidValidity_On_Rename)
	addTest(Mailbox_ListMessages)
	addTest(Mailbox_ListMessages_Body)
	addTest(Mailbox_ListMessages_Meta)
	addTest(Mailbox_ListMessages_Multi)
	addTest(Mailbox_SearchMessages_Body)
	addTest(Mailbox_SearchMessages_Flags)
	addTest(Mailbox_SetMessageFlags)
	addTest(Mailbox_MonotonicUid)
	addTest(Mailbox_Expunge)
	addTest(Mailbox_CopyMessages)

	addTest(Mailbox_ExpungeUpdate)
	addTest(Mailbox_StatusUpdate)
	addTest(Mailbox_StatusUpdate_Copy)
	addTest(Mailbox_StatusUpdate_Move)
	addTest(Mailbox_MessageUpdate)

	// MOVE extension
	addTest(Mailbox_MoveMessages)

	// APPEND-LIMIT extension
	addTest(Backend_AppendLimit)
	addTest(User_AppendLimit)
	addTest(Mailbox_AppendLimit)
}

func TestInit(t *testing.T, newBackend NewBackFunc, closeBackend CloseBackFunc) {
	b := newBackend()
	closeBackend(b)
}
