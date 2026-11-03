//go:build testcontainers

package imapsql

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-message/textproto"
	backendtests "github.com/foxcpp/go-imap-backend-tests"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"gotest.tools/assert"
)

func TestWithPostgres(t *testing.T) {
	ctx := t.Context()

	// TODO: Figure out how to support TEST_DB/TEST_DSN with snapshots
	// for CI runs.

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("imapsql"),
		postgres.WithUsername("imapsql"),
		postgres.WithPassword("imapsqlpassword"),
		postgres.BasicWaitStrategies(),
	)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	})
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}

	postgresContainerDB := func(c *testBackendContainer) {
		dsn, err := postgresContainer.ConnectionString(t.Context(), "sslmode=disable")
		assert.NilError(t, err)

		c.Driver = "postgres"
		c.DSN = dsn
	}

	be := setupTestBackend(t, postgresContainerDB)
	assert.NilError(t, be.Close())
	assert.NilError(t, postgresContainer.Snapshot(t.Context()))

	backendtests.RunTests(t, func() backendtests.Backend {
		be := setupTestBackend(t, postgresContainerDB)
		return be.Backend
	}, func(backend backendtests.Backend) {
		assert.NilError(t, backend.(*Backend).Close())
		assert.NilError(t, postgresContainer.Restore(t.Context()))
	})

	t.Run("RecentClaimDoesNotDeadlockExpunge", func(t *testing.T) {
		t.Cleanup(func() {
			postgresContainer.Restore(context.Background())
		})
		be := setupTestBackend(t, postgresContainerDB)

		usr := be.user()

		d := be.NewDelivery()
		assert.NilError(t, d.AddRcpt(usr.Username(), textproto.Header{}))
		assert.NilError(t, d.BodyRaw(strings.NewReader(testMsg)))
		assert.NilError(t, d.Commit())

		tx, err := be.db.Begin(false)
		assert.NilError(t, err)
		defer tx.Rollback()

		_, err = tx.Exec(`SET LOCAL lock_timeout = '2s'`)
		assert.NilError(t, err)

		_, err = tx.Exec(`UPDATE msgs SET mark = mark WHERE mboxId = (SELECT inboxId FROM users WHERE username = $1)`, usr.Username())
		assert.NilError(t, err)

		selected := make(chan error, 1)
		go func() {
			_, box, err := usr.GetMailbox("INBOX", false, &noopConn{})
			if err == nil {
				assert.NilError(t, box.Close())
			}
			selected <- err
		}()

		deadline := time.Now().Add(5 * time.Second)
		for {
			var blocked bool
			err := be.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			AND (query ILIKE '%recent%' OR query ILIKE '%msgId%'))`).Scan(&blocked)
			if err != nil {
				assert.NilError(t, err)
			}
			if blocked {
				break
			}
			if !time.Now().Before(deadline) {
				assert.Assert(t, false, "recent claim did not wait for locked message")
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		_, err = tx.Exec(`UPDATE mboxes SET msgsCount = msgsCount WHERE id = (SELECT inboxId FROM users WHERE username = 'deadlock-test')`)
		_ = tx.Rollback()
		selectErr := <-selected
		assert.NilError(t, err)
		assert.NilError(t, selectErr)
	})

	// XXX: Add PostgreSQL-specific tests here.
}
