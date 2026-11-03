//go:build testcontainers

package imapsql

import (
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	backendtests "github.com/foxcpp/go-imap-backend-tests"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func initPostgresTestBackend(t *testing.T, cnt *postgres.PostgresContainer) backendtests.Backend {
	randSrc := rand.NewSource(0)
	prng := rand.New(randSrc)

	storeDir := filepath.Join(t.TempDir(), "store")
	if err := os.MkdirAll(storeDir, os.ModeDir|os.ModePerm); err != nil {
		panic(err)
	}

	var log Logger
	if testing.Verbose() {
		log = globalLogger{}
	} else {
		log = DummyLogger{}
	}

	dsn, err := cnt.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)

	b, err := New("postgres", dsn, &FSStore{Root: storeDir}, Opts{
		PRNG: prng,
		Log:  log,
	})
	if err != nil {
		panic(err)
	}
	return b
}

func cleanPostgresBackend(t *testing.T, bi backendtests.Backend, cnt *postgres.PostgresContainer) {
	b := bi.(*Backend)
	if os.Getenv("PRESERVE_DB") != "1" && os.Getenv("PRESERVE_SQLITE3_DB") != "1" {
		require.NoError(t, cnt.Restore(t.Context()))

		if err := os.RemoveAll(b.extStore.(*FSStore).Root); err != nil {
			log.Println(err)
		}
	}
	b.Close()
}

func TestWithPostgres(t *testing.T) {
	ctx := t.Context()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("imapsql"),
		postgres.WithUsername("imapsql"),
		postgres.WithPassword("imapsqlpassword"),
		postgres.BasicWaitStrategies(),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}

	// Take a snapshot of an empty DB so we can wipe it easier for each case.
	require.NoError(t, postgresContainer.Snapshot(t.Context()))

	backendtests.RunTests(t, func() backendtests.Backend {
		return initPostgresTestBackend(t, postgresContainer)
	}, func(backend backendtests.Backend) {
		cleanPostgresBackend(t, backend, postgresContainer)
	})
}
