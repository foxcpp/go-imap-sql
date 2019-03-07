### IMAP backend test suite

Tests moved to separate package because they are reusable.

Tested backend must implement IMAPUsersDB interface defined in go-sqlmail package.
Tests for IMAP extensions may require other interfaces, check messages printed
for skipped tests.

Just call `testsuite.RunTests(t, newBackend, closeBackend)` from your backend (or
`backend_test`) package.  Each invocation of newBackend callback should provide
clean instance of backend (e.g. with empty storage, etc).  closeBackend will be
called for backend after usage. New instance is created for each test.
