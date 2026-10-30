# Agent guidance

## Layout and boundaries

- The repository is a single Go module (`github.com/foxcpp/go-imap-sql`); the root package is the SQL-backed IMAP storage backend.
- `cmd/imapd` is the unauthenticated IMAP daemon entrypoint; `cmd/imapsql-ctl` is the low-level database management CLI.
- `backend.go`, `user.go`, `mailbox.go`, `delivery.go`, and `fetch.go` contain the main storage/application paths; SQL schema and database-specific behavior live in `schema.go`, `sql.go`, and `dbhacks.go`.
- The backend does not authenticate users. Its `Login` password is intentionally not checked; authentication must be supplied by a wrapper/server integration.

## Tests and verification

- Run the default suite with `go test ./...`. Test helpers default to an in-memory SQLite database when `TEST_DB` is unset.
- Run one test or package with standard Go selectors, for example `go test . -run '^TestUserCaseInsensitivity$'` or `go test ./cmd/...`.
- To exercise a configured database, set `TEST_DB` and `TEST_DSN`; CI uses `TEST_DB=sqlite3 TEST_DSN=':memory:'` or PostgreSQL with `TEST_DB=postgres TEST_DSN='user=postgres dbname=sqlmail_test sslmode=disable'`.
- The CI-equivalent stress check is `go test ./... -race -coverprofile=coverage.txt -covermode=atomic -tags "$TEST_DB" -count 8 -p "$PARALLEL_TESTS"`; PostgreSQL requires a running server/database.
- Verify dependencies with `go mod verify`; build both command packages with `go build ./cmd/...`; lint with a golangci-lint version compatible with the legacy `.golangci.yml` (the currently installed newer tool rejects its missing `version` field before linting).
- SQLite tests use `github.com/mattn/go-sqlite3`, so normal SQLite verification requires CGO. `PRESERVE_SQLITE3_DB=1` keeps a temporary on-disk database for debugging instead of using `:memory:`.
- Changes that affect standard protocol inputs or outputs, change semantics of commands must be cross-verified for correctness against represtive RFC specifications. If unsure - ask the user for RFC reference. Use either tool available to fetch RFC text (RFC MCP, or Web Fetch from http://www.rfc-editor.org/info/rfcNNNN).

## Implementation constraints

- `New` passes the SQL driver and DSN through to `database/sql`, then initializes/upgrades the schema; do not assume all SQL dialects behave like SQLite. SQLite, PostgreSQL, and experimental/legacy database support are documented in `README.md`.
- The generated-looking `bodystructure_easyjson.go` and `cached_header_easyjson.go` files are explicitly marked as “Patched by hand”; do not regenerate or replace them casually.
- `imapsql-ctl` writes directly to the database. Avoid mailbox writes while a server/client is connected unless the CLI’s `--unsafe` warning and notification implications are understood.
- The repository’s documented minimum is Go 1.13 because of error inspection APIs, despite older historical CI configuration; use a current supported Go toolchain.
