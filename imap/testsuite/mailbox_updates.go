package testsuite

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	move "github.com/emersion/go-imap-move"
	"github.com/emersion/go-imap/backend"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

func makeMsgSlots(count int) (res []uint32) {
	res = make([]uint32, count)
	for i := range res {
		res[i] = uint32(i + 1)
	}
	return
}

func checkExpungeEvents(t *testing.T, upds <-chan backend.Update, slots *[]uint32, shouldBeLeft uint32) {
	failTick := time.NewTimer(2 * time.Second)
	t.Helper()
	if uint32(len(*slots)) == shouldBeLeft {
		return
	}
	for {
		select {
		case <-failTick.C:
			t.Fatal("ExpungeUpdate's for all messages are not sent in 2 seconds. Remaining slots:", len(*slots))
		case upd := <-upds:
			switch upd := upd.(type) {
			case *backend.ExpungeUpdate:
				if upd.SeqNum > uint32(len(*slots)) {
					t.Errorf("Update's SeqNum is out of range: %v > %v", upd.SeqNum, len(*slots))
				} else if upd.SeqNum == 0 {
					t.Error("Update's SeqNum is zero.")
				} else {
					*slots = append((*slots)[:upd.SeqNum-1], (*slots)[upd.SeqNum:]...)
					//t.Logf("Got ExpungeUpdate, SeqNum = %d, remaining slots = %d\n", upd.SeqNum, len(*slots))
					if uint32(len(*slots)) == shouldBeLeft {
						return
					}
				}
			default:
				t.Errorf("Expunge should not generate non-expunge updates (%T): %#v", upd, upd)
			}
		}
	}
}

func consumeUpdates(t *testing.T, ch <-chan backend.Update, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-ch:
			//t.Logf("Skipping %#v", upd)
		default:
			t.Log("consumeCreationUpdates failed to consume all expected updates")
			return
		}
	}
}

func Mailbox_StatusUpdate(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)

	updater, ok := b.(backend.BackendUpdater)
	if !ok {
		t.Skip("Backend doesn't supports unilateral updates (need backend.BackendUpdater interface)")
		t.SkipNow()
	}
	upds := updater.Updates()

	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	mbox := getMbox(t, u)

	for i := uint32(1); i <= uint32(5); i++ {
		createMsgs(t, mbox, 1)
		upd := readUpdate(t, upds)
		switch upd := upd.(type) {
		case *backend.MailboxUpdate:
			assert.Check(t, is.Equal(upd.Messages, i), "Wrong amount of messages in mailbox reported in update")
			assert.Check(t, is.Equal(upd.Recent, i), "Wrong amount of recent messages in mailbox reported in update")
		default:
			t.Errorf("Non-mailbox update sent by backend: %#v\n", upd)
		}
	}
}

func Mailbox_StatusUpdate_Copy(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)

	updater, ok := b.(backend.BackendUpdater)
	if !ok {
		t.Skip("Backend doesn't supports unilateral updates (need backend.BackendUpdater interface)")
		t.SkipNow()
	}
	upds := updater.Updates()

	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	srcMbox := getMbox(t, u)
	tgtMbox := getMbox(t, u)

	createMsgs(t, srcMbox, 3)
	consumeUpdates(t, upds, 3)

	seq, _ := imap.ParseSeqSet("2:3")
	assert.NilError(t, srcMbox.CopyMessages(false, seq, tgtMbox.Name()))

	upd := readUpdate(t, upds)
	switch upd := upd.(type) {
	case *backend.MailboxUpdate:
		assert.Check(t, is.Equal(upd.Mailbox(), tgtMbox.Name()), "Update is for wrong mailbox")
		assert.Check(t, is.Equal(upd.Messages, uint32(2)), "Wrong amount of messages in mailbox reported in update")
		assert.Check(t, is.Equal(upd.Recent, uint32(2)), "Wrong amount of recent messages in mailbox reported in update")
	default:
		t.Errorf("Non-mailbox update sent by backend: %#v\n", upd)
	}
}

func Mailbox_StatusUpdate_Move(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)

	updater, ok := b.(backend.BackendUpdater)
	if !ok {
		t.Skip("Backend doesn't supports unilateral updates (need backend.BackendUpdater interface)")
		t.SkipNow()
	}
	upds := updater.Updates()

	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	srcMbox := getMbox(t, u)
	tgtMbox := getMbox(t, u)

	createMsgs(t, srcMbox, 3)
	consumeUpdates(t, upds, 3)

	moveMbox, ok := srcMbox.(move.Mailbox)
	if !ok {
		t.Skip("Backend doesn't supports MOVE (need move.Mailbox interface)")
		t.SkipNow()
	}

	seq, _ := imap.ParseSeqSet("2:3")
	assert.NilError(t, moveMbox.MoveMessages(false, seq, tgtMbox.Name()))

	// We expect 1 status update for target mailbox and two expunge updates
	// for source mailbox.
	msgs := makeMsgSlots(3)

	for i := 0; i < 3; i++ {
		upd := readUpdate(t, upds)
		if upd.Mailbox() == tgtMbox.Name() {
			mboxUpd, ok := upd.(*backend.MailboxUpdate)
			if !ok {
				t.Fatal("Non-MailboxUpdate received for target mailbox")
			}

			assert.Check(t, is.Equal(mboxUpd.Messages, uint32(2)), "Wrong amount of messages in mailbox reported in update for target")
		} else if upd.Mailbox() == srcMbox.Name() {
			expungeUpd, ok := upd.(*backend.ExpungeUpdate)
			if !ok {
				t.Fatalf("Non-ExpungeUpdate received for source mailbox: %#v", upd)
			}

			if expungeUpd.SeqNum > uint32(len(msgs)) {
				t.Errorf("Update's SeqNum is out of range: %v > %v", expungeUpd.SeqNum, len(msgs))
			} else if expungeUpd.SeqNum == 0 {
				t.Error("Update's SeqNum is zero.")
			} else {
				//t.Logf("Got ExpungeUpdate, SeqNum = %d, remaining slots = %d\n", expungeUpd.SeqNum, len(msgs))
				msgs = append(msgs[:expungeUpd.SeqNum-1], msgs[expungeUpd.SeqNum:]...)
			}
		} else {
			t.Fatal("Update received for nither target nor source mailbox")
		}
	}

	assert.Check(t, is.DeepEqual(msgs, []uint32{1}), "Wrong sequence of expunge updates received")
}

func Mailbox_MessageUpdate(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)

	updater, ok := b.(backend.BackendUpdater)
	if !ok {
		t.Skip("Backend doesn't supports unilateral updates (need backend.BackendUpdater interface)")
		t.SkipNow()
	}
	upds := updater.Updates()

	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	testFlagsUpdate := func(
		seqset string, expectedUpdates int,
		initialFlags map[uint32][]string, op imap.FlagsOp,
		opArg []string, expectedNewFlags map[uint32][]string) {

		t.Run(fmt.Sprintf("seqset=%v op=%v opArg=%v", seqset, op, opArg), func(t *testing.T) {
			mbox := getMbox(t, u)

			for i := 1; i <= len(initialFlags); i++ {
				assert.NilError(t, mbox.CreateMessage(initialFlags[uint32(i)], time.Now(), strings.NewReader(testMsg)))
				consumeUpdates(t, upds, 1)
			}

			seq, _ := imap.ParseSeqSet(seqset)
			assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, op, opArg))

			for i := 0; i < expectedUpdates; i++ {
				upd := readUpdate(t, upds)
				switch upd := upd.(type) {
				case *backend.MessageUpdate:
					flags, ok := expectedNewFlags[upd.SeqNum]
					if !ok {
						t.Error("Unexpected update for SeqNum =", upd.SeqNum)
					}

					sort.Strings(flags)
					sort.Strings(upd.Flags)

					if !assert.Check(t, is.DeepEqual(flags, upd.Flags), "Flags mismatch on message %d", upd.SeqNum) {
						t.Log("upd.Flags:", upd.Flags)
						t.Log("Reference flag set:", flags)
					}
				default:
					t.Errorf("Non-message update sent by backend: %#v\n", upd)
				}
			}
		})
	}

	cases := []struct {
		seqset           string
		expectedUpdates  int
		initialFlags     map[uint32][]string
		op               imap.FlagsOp
		opArg            []string
		expectedNewFlags map[uint32][]string
	}{
		{
			"1,3,5", 3, map[uint32][]string{
				1: []string{"t1-1", "t1-2"},
				2: []string{"t2-3", "t2-4"},
				3: []string{"t3-5", "t3-6"},
				4: []string{"t4-7", "t4-8"},
				5: []string{"t5-9", "t5-10"},
			},
			imap.SetFlags, []string{"t0-1", "t0-2"},
			map[uint32][]string{
				1: []string{imap.RecentFlag, "t0-1", "t0-2"},
				3: []string{imap.RecentFlag, "t0-1", "t0-2"},
				5: []string{imap.RecentFlag, "t0-1", "t0-2"},
			},
		},
		{
			"1,3,5", 3, map[uint32][]string{
				1: []string{"t1-1", "t1-2"},
				2: []string{"t2-3", "t2-4"},
				3: []string{"t3-5", "t3-6"},
				4: []string{"t4-7", "t4-8"},
				5: []string{"t5-9", "t5-10"},
			},
			imap.AddFlags, []string{"t0-1", "t0-2"},
			map[uint32][]string{
				1: []string{imap.RecentFlag, "t0-1", "t0-2", "t1-1", "t1-2"},
				3: []string{imap.RecentFlag, "t0-1", "t0-2", "t3-5", "t3-6"},
				5: []string{imap.RecentFlag, "t0-1", "t0-2", "t5-10", "t5-9"},
			},
		},
		{
			"2,3,5", 3, map[uint32][]string{
				1: []string{"t1-1", "t1-2"},
				2: []string{"t0-0", "t2-4"},
				3: []string{"t3-5", "t0-0"},
				4: []string{"t4-7", "t4-8"},
				5: []string{"t0-0", "t5-10"},
			},
			imap.RemoveFlags, []string{"t0-0"},
			map[uint32][]string{
				2: []string{imap.RecentFlag, "t2-4"},
				3: []string{imap.RecentFlag, "t3-5"},
				5: []string{imap.RecentFlag, "t5-10"},
			},
		},
	}

	if os.Getenv("SHUFFLE_CASES") == "1" {
		rand.Shuffle(len(cases), func(i, j int) {
			cases[i], cases[j] = cases[j], cases[i]
		})
	}

	for _, case_ := range cases {
		testFlagsUpdate(case_.seqset, case_.expectedUpdates, case_.initialFlags, case_.op, case_.opArg, case_.expectedNewFlags)
	}
}

func readUpdate(t *testing.T, upds <-chan backend.Update) backend.Update {
	timer := time.NewTimer(2 * time.Second)
	select {
	case upd := <-upds:
		timer.Stop()
		return upd
	case <-timer.C:
		t.Fatal("Test timeout")
	}
	return nil
}

func Mailbox_ExpungeUpdate(t *testing.T, newBack NewBackFunc, closeBack CloseBackFunc) {
	b := newBack()
	defer closeBack(b)

	updater, ok := b.(backend.BackendUpdater)
	if !ok {
		t.Skip("Backend doesn't supports unilateral updates (need backend.BackendUpdater interface)")
		t.SkipNow()
	}
	upds := updater.Updates()

	u := getUser(t, b)
	defer assert.NilError(t, u.Logout())

	testSlots := func(msgsCount int, seqset string, matchedMsgs int, expectedSlots []uint32) {
		t.Run(seqset, func(t *testing.T) {
			mbox := getMbox(t, u)
			createMsgs(t, mbox, msgsCount)
			consumeUpdates(t, upds, msgsCount)
			msgs := makeMsgSlots(msgsCount)

			seq, _ := imap.ParseSeqSet(seqset)
			assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.AddFlags, []string{imap.DeletedFlag}))
			consumeUpdates(t, upds, matchedMsgs)

			assert.NilError(t, mbox.Expunge())
			checkExpungeEvents(t, upds, &msgs, uint32(msgsCount-matchedMsgs))

			assert.DeepEqual(t, msgs, expectedSlots)
		})
	}

	cases := []struct {
		msgsCount     int
		seqset        string
		matchedMsgs   int
		expectedSlots []uint32
	}{
		{5, "*", 5, []uint32{}},
		{5, "1", 1, []uint32{2, 3, 4, 5}},
		{5, "2,1,5", 3, []uint32{3, 4}},
	}

	if os.Getenv("SHUFFLE_CASES") == "1" {
		rand.Shuffle(len(cases), func(i, j int) {
			cases[i], cases[j] = cases[j], cases[i]
		})
	}

	for _, case_ := range cases {
		testSlots(case_.msgsCount, case_.seqset, case_.matchedMsgs, case_.expectedSlots)
	}

	// Make sure backend returns seqnums, not UIDs.
	t.Run("Not UIDs", func(t *testing.T) {
		mbox := getMbox(t, u)
		createMsgs(t, mbox, 6)
		consumeUpdates(t, upds, 6)

		seq, _ := imap.ParseSeqSet("1")
		assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.AddFlags, []string{imap.DeletedFlag}))
		assert.NilError(t, mbox.Expunge())
		consumeUpdates(t, upds, 2)

		msgs := makeMsgSlots(5)
		seq, _ = imap.ParseSeqSet("2,1,5")
		assert.NilError(t, mbox.UpdateMessagesFlags(false, seq, imap.AddFlags, []string{imap.DeletedFlag}))
		consumeUpdates(t, upds, 3)

		assert.NilError(t, mbox.Expunge())
		checkExpungeEvents(t, upds, &msgs, uint32(2))

		assert.DeepEqual(t, msgs, []uint32{3, 4})
	})
}
