package imapsql

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"strings"
	"sync"
	"time"

	"errors"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
)

var (
	ErrUserAlreadyExists = errors.New("imap: user already exists")
	ErrUserDoesntExists  = errors.New("imap: user doesn't exists")
)

type Rand interface {
	Uint32() uint32
}

type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Debugf(format string, v ...interface{})
	Debugln(v ...interface{})
}

// Opts structure specifies additional settings that may be set
// for backend.
//
// Please use names to reference structure members on creation,
// fields may be reordered or added without major version increment.
type Opts struct {
	// Adding unexported name to structures makes it impossible to
	// reference fields without naming them explicitly.
	disallowUnnamedFields struct{}

	// Maximum amount of bytes that backend will accept.
	// Intended for use with APPENDLIMIT extension.
	// nil value means no limit, 0 means zero limit (no new messages allowed)
	MaxMsgBytes *uint32

	// Controls when channel returned by Updates should be created.
	// If set to false - channel will be created before NewBackend returns.
	// If set to true - channel will be created upon first call to Updates.
	// Second is useful for tests that don't consume values from Updates
	// channel.
	LazyUpdatesInit bool

	// UpdatesChan allows to pass custom channel object used for unilateral
	// updates dispatching.
	//
	// You can use this to change default updates buffer size (20) or to split
	// initializaton into phases (which allows to break circular dependencies
	// if you need updates channel before database initialization).
	UpdatesChan chan backend.Update

	// Custom randomness source for UIDVALIDITY values generation.
	PRNG Rand

	// (SQLite3 only) Don't force WAL journaling mode.
	NoWAL bool

	// (SQLite3 only) Use different value for busy_timeout. Default is 50000.
	// To set to 0, use -1 (you probably don't want this).
	BusyTimeout int

	// (SQLite3 only) Use EXCLUSIVE locking mode.
	ExclusiveLock bool

	// (SQLite3 only) Change page cache size. Positive value indicates cache
	// size in pages, negative in KiB. If set 0 - SQLite default will be used.
	CacheSize int

	// (SQLite3 only) Repack database file into minimal amount of disk space on
	// Close.
	// It runs VACUUM and PRAGMA wal_checkpoint(TRUNCATE).
	// Failures of these operations are ignored and don't affect return value
	// of Close.
	MinimizeOnClose bool

	// Hash algorithm to use for authentication passwords when no algorithm is
	// explicitly specified.
	// "sha3-512" and "bcrypt" are supported out of the box. "bcrypt" is used
	// by default.
	// Support for aditional algoritms can be enabled using EnableHashAlgo.
	DefaultHashAlgo string

	// Bcrypt cost value to use when computing password hashes.
	// Default is 10. Can't be smaller than 4, can't be bigger than 31.
	//
	// It is safe to change it, existing records will not be affected.
	BcryptCost int

	// Compression algorithm to use for new messages. Empty string means no compression.
	//
	// Algorithms should be registered before using RegisterCompressionAlgo.
	CompressAlgo string

	// CompressAlgoParams is passed directly to compression algorithm without changes.
	CompressAlgoParams string

	Log Logger
}

type Backend struct {
	db       db
	extStore ExternalStore

	// Opts structure used to construct this Backend object.
	//
	// For most cases it is safe to change options while backend is serving
	// requests.
	// Options that should NOT be changed while backend is processing commands:
	// - PRNG
	// - CompressAlgoParams
	// Changes for the following options have no effect after backend initialization:
	// - CompressAlgo
	// - ExclusiveLock
	// - CacheSize
	// - NoWAL
	// - UpdatesChan
	Opts Opts

	// database/sql.DB object created by New.
	DB *sql.DB

	childrenExt   bool
	specialUseExt bool

	prng           Rand
	hashAlgorithms map[string]hashAlgorithm
	compressAlgo   CompressionAlgo

	updates chan backend.Update
	// updates channel is lazily initalized, so we need to ensure thread-safety.
	updatesLck sync.Mutex

	// Shitton of pre-compiled SQL statements.
	userCreds          *sql.Stmt
	listUsers          *sql.Stmt
	addUser            *sql.Stmt
	delUser            *sql.Stmt
	setUserPass        *sql.Stmt
	listMboxes         *sql.Stmt
	listSubbedMboxes   *sql.Stmt
	createMboxExistsOk *sql.Stmt
	createMbox         *sql.Stmt
	deleteMbox         *sql.Stmt
	renameMbox         *sql.Stmt
	renameMboxChilds   *sql.Stmt
	getMboxAttrs       *sql.Stmt
	setSubbed          *sql.Stmt
	uidNextLocked      *sql.Stmt
	uidNext            *sql.Stmt
	hasChildren        *sql.Stmt
	uidValidity        *sql.Stmt
	msgsCount          *sql.Stmt
	recentCount        *sql.Stmt
	firstUnseenSeqNum  *sql.Stmt
	deletedSeqnums     *sql.Stmt
	expungeMbox        *sql.Stmt
	mboxId             *sql.Stmt
	addMsg             *sql.Stmt
	copyMsgsUid        *sql.Stmt
	copyMsgFlagsUid    *sql.Stmt
	copyMsgsSeq        *sql.Stmt
	copyMsgFlagsSeq    *sql.Stmt
	massClearFlagsUid  *sql.Stmt
	massClearFlagsSeq  *sql.Stmt
	msgFlagsUid        *sql.Stmt
	msgFlagsSeq        *sql.Stmt
	usedFlags          *sql.Stmt
	listMsgUids        *sql.Stmt

	addRecentToLast *sql.Stmt

	// 'mark' column for messages is used to keep track of messages selected
	// by sequence numbers during operations that may cause seqence numbers to
	// change (e.g. message deletion)
	//
	// Consider following request: Delete messages with seqnum 1 and 3.
	// Naive implementation will delete 1st and then 3rd messages in mailbox.
	// However, after first operation 3rd message will become 2nd and
	// code will end up deleting the wrong message (4th actually).
	//
	// Solution is to "mark" 1st and 3rd message and then delete all "marked"
	// message.
	//
	// One could use \Deleted flag for this purpose, but this
	// requires more expensive operations at SQL engine side, so 'mark' column
	// is basically a optimization.

	// For MOVE extension
	markUid   *sql.Stmt
	markSeq   *sql.Stmt
	delMarked *sql.Stmt

	markedSeqnums *sql.Stmt

	// For APPEND-LIMIT extension
	setUserMsgSizeLimit *sql.Stmt
	userMsgSizeLimit    *sql.Stmt
	setMboxMsgSizeLimit *sql.Stmt
	mboxMsgSizeLimit    *sql.Stmt

	searchFetch      *sql.Stmt
	searchFetchNoSeq *sql.Stmt

	flagsSearchStmtsLck   sync.RWMutex
	flagsSearchStmtsCache map[string]*sql.Stmt
	fetchStmtsLck         sync.RWMutex
	fetchStmtsCache       map[string]*sql.Stmt
	addFlagsStmtsLck      sync.RWMutex
	addFlagsStmtsCache    map[string]*sql.Stmt
	remFlagsStmtsLck      sync.RWMutex
	remFlagsStmtsCache    map[string]*sql.Stmt

	// extkeys table
	addExtKey             *sql.Stmt
	decreaseRefForMarked  *sql.Stmt
	decreaseRefForDeleted *sql.Stmt
	incrementRefUid       *sql.Stmt
	incrementRefSeq       *sql.Stmt
	zeroRef               *sql.Stmt
	zeroRefUser           *sql.Stmt
	refUser               *sql.Stmt
	deleteZeroRef         *sql.Stmt
	deleteUserRef         *sql.Stmt
	decreaseRefForMbox    *sql.Stmt

	// Used by Delivery.SpecialMailbox.
	specialUseMbox *sql.Stmt

	setSeenFlagUid   *sql.Stmt
	setSeenFlagSeq   *sql.Stmt
	increaseMsgCount *sql.Stmt
	decreaseMsgCount *sql.Stmt

	setInboxId *sql.Stmt

	sqliteOptimizeLoopStop chan struct{}
}

var defaultPassHashAlgo = "bcrypt"

// New creates new Backend instance using provided configuration.
//
// driver and dsn arguments are passed directly to sql.Open.
//
// Note that it is not safe to create multiple Backend instances working with
// the single database as they need to keep some state synchronized and there
// is no measures for this implemented in go-imap-sql.
func New(driver, dsn string, extStore ExternalStore, opts Opts) (*Backend, error) {
	b := &Backend{
		fetchStmtsCache:       make(map[string]*sql.Stmt),
		flagsSearchStmtsCache: make(map[string]*sql.Stmt),
		addFlagsStmtsCache:    make(map[string]*sql.Stmt),
		remFlagsStmtsCache:    make(map[string]*sql.Stmt),
		hashAlgorithms:        make(map[string]hashAlgorithm),

		sqliteOptimizeLoopStop: make(chan struct{}),

		extStore: extStore,
		Opts:     opts,
	}
	var err error

	if b.Opts.CompressAlgo != "" {
		impl, ok := compressionAlgos[b.Opts.CompressAlgo]
		if !ok {
			return nil, fmt.Errorf("New: unknown compression algorithm: %s", b.Opts.CompressAlgo)
		}

		b.compressAlgo = impl
	} else {
		b.compressAlgo = nullCompression{}
	}

	b.Opts = opts
	if !b.Opts.LazyUpdatesInit {
		b.updates = b.Opts.UpdatesChan
		if b.updates == nil {
			b.updates = make(chan backend.Update, 20)
		}
	}

	if b.Opts.Log == nil {
		b.Opts.Log = globalLogger{}
	}

	b.enableDefaultHashAlgs()
	if b.Opts.DefaultHashAlgo == "" {
		b.Opts.DefaultHashAlgo = defaultPassHashAlgo
	}
	if b.Opts.BcryptCost == 0 {
		b.Opts.BcryptCost = 10
	}

	if b.Opts.PRNG != nil {
		b.prng = opts.PRNG
	} else {
		b.prng = mathrand.New(mathrand.NewSource(time.Now().Unix()))
	}

	if driver == "sqlite3" {
		dsn = b.addSqlite3Params(dsn)
	}

	b.db.driver = driver
	b.db.dsn = dsn

	b.db.DB, err = sql.Open(driver, dsn)
	if err != nil {
		return nil, wrapErr(err, "NewBackend (open)")
	}
	b.DB = b.db.DB

	ver, err := b.schemaVersion()
	if err != nil {
		return nil, wrapErr(err, "NewBackend (schemaVersion)")
	}
	// Zero version indicates "empty database".
	if ver > SchemaVersion {
		return nil, fmt.Errorf("incompatible database schema, too new (%d > %d)", ver, SchemaVersion)
	}
	if ver < SchemaVersion && ver != 0 {
		b.Opts.Log.Printf("Upgrading database schema (from %d to %d)", ver, SchemaVersion)
		if err := b.upgradeSchema(ver); err != nil {
			return nil, wrapErr(err, "NewBackend (schemaUpgrade)")
		}
	}
	if err := b.setSchemaVersion(SchemaVersion); err != nil {
		return nil, wrapErr(err, "NewBackend (setSchemaVersion)")
	}

	if err := b.configureEngine(); err != nil {
		return nil, wrapErr(err, "NewBackend (configureEngine)")
	}

	if err := b.initSchema(); err != nil {
		return nil, wrapErr(err, "NewBackend (initSchema)")
	}
	if err := b.prepareStmts(); err != nil {
		return nil, wrapErr(err, "NewBackend (prepareStmts)")
	}

	for _, item := range [...]imap.FetchItem{
		imap.FetchFlags, imap.FetchEnvelope,
		imap.FetchBodyStructure, "BODY[]", "BODY[HEADER.FIELDS (From To)]"} {

		if _, err := b.getFetchStmt(true, []imap.FetchItem{item}); err != nil {
			return nil, wrapErrf(err, "fetchStmt prime (%s, uid=true)", item)
		}
		if _, err := b.getFetchStmt(false, []imap.FetchItem{item}); err != nil {
			return nil, wrapErrf(err, "fetchStmt prime (%s, uid=false)", item)
		}
	}

	if b.db.driver == "sqlite3" {
		go b.sqliteOptimizeLoop()
	}

	return b, nil
}

// EnableChildrenExt enables generation of /HasChildren and /HasNoChildren
// attributes for mailboxes. It should be used only if server advertises
// CHILDREN extension support (see children subpackage).
func (b *Backend) EnableChildrenExt() bool {
	b.childrenExt = true
	return true
}

// EnableSpecialUseExt enables generation of special-use attributes for
// mailboxes. It should be used only if server advertises SPECIAL-USE extension
// support (see go-imap-specialuse).
func (b *Backend) EnableSpecialUseExt() bool {
	b.specialUseExt = true
	return true
}

func (b *Backend) sqliteOptimizeLoop() {
	t := time.NewTicker(5 * time.Hour)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.Opts.Log.Debugln("running SQLite query planer optimization...")
			b.db.Exec(`PRAGMA optimize`)
			b.Opts.Log.Debugln("completed SQLite query planer optimization")
		case <-b.sqliteOptimizeLoopStop:
			return
		}
	}
}

func (b *Backend) Close() error {
	if b.db.driver == "sqlite3" {
		// These operations are not critical, so it's not a problem if they fail.
		if b.Opts.MinimizeOnClose {
			b.db.Exec(`VACUUM`)
			b.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
		}

		b.sqliteOptimizeLoopStop <- struct{}{}
		b.db.Exec(`PRAGMA optimize`)
	}

	if b.updates != nil {
		close(b.updates)
	}

	return b.db.Close()
}

func (b *Backend) Updates() <-chan backend.Update {
	if b.Opts.LazyUpdatesInit && b.updates == nil {
		b.updatesLck.Lock()
		defer b.updatesLck.Unlock()

		if b.updates == nil {
			b.updates = make(chan backend.Update, 20)
		}
	}
	return b.updates
}

func (b *Backend) getUserCreds(tx *sql.Tx, username string) (id uint64, inboxId uint64, hashAlgo string, passHash []byte, passSalt []byte, err error) {
	var row *sql.Row
	if tx != nil {
		row = tx.Stmt(b.userCreds).QueryRow(username)
	} else {
		row = b.userCreds.QueryRow(username)
	}
	var passHashHex, passSaltHex sql.NullString
	if err := row.Scan(&id, &inboxId, &passHashHex, &passSaltHex); err != nil {
		return 0, 0, "", nil, nil, err
	}

	if !passHashHex.Valid || !passSaltHex.Valid {
		return id, inboxId, "", nil, nil, nil
	}

	hashHexParts := strings.Split(passHashHex.String, ":")
	if len(hashHexParts) != 2 {
		return id, 0, "", nil, nil, fmt.Errorf("malformed database column value for password, need algo:hexhash, got %s", passHashHex.String)
	}

	hashAlgo = hashHexParts[0]
	passHash = []byte(hashHexParts[1])
	passSalt, err = hex.DecodeString(passSaltHex.String)
	if err != nil {
		return 0, 0, "", nil, nil, err
	}

	return id, inboxId, hashAlgo, passHash, passSalt, nil
}

// CreateUser creates user account with specified credentials.
//
// This method can fail if used crypto/rand fails to create enough entropy.
// It is error to create account with username that already exists.
// ErrUserAlreadyExists will be returned in this case.
func (b *Backend) CreateUser(username, password string) error {
	_, _, err := b.createUser(nil, strings.ToLower(username), b.Opts.DefaultHashAlgo, &password)
	return err
}

func (b *Backend) CreateUserWithHash(username, hashAlgo, password string) error {
	_, _, err := b.createUser(nil, strings.ToLower(username), hashAlgo, &password)
	return err
}

// CreateUserNoPass creates new user account without a password set.
//
// It will be unable to log in until SetUserPassword is called for it.
func (b *Backend) CreateUserNoPass(username string) error {
	_, _, err := b.createUser(nil, strings.ToLower(username), b.Opts.DefaultHashAlgo, nil)
	return err
}

func (b *Backend) createUser(tx *sql.Tx, username string, passHashAlgo string, password *string) (uid, inboxId uint64, err error) {
	var passHash, passSalt sql.NullString
	if password != nil {
		var err error
		passHash.Valid = true
		passSalt.Valid = true
		passHash.String, passSalt.String, err = b.hashCredentials(passHashAlgo, *password)
		if err != nil {
			return 0, 0, wrapErr(err, "CreateUser")
		}
	}

	var shouldCommit bool
	if tx == nil {
		var err error
		tx, err = b.db.Begin(false)
		if err != nil {
			return 0, 0, wrapErr(err, "CreateUser")
		}
		defer tx.Rollback()
		shouldCommit = true
	}

	_, err = tx.Stmt(b.addUser).Exec(username, passHash, passSalt)
	if err != nil && isForeignKeyErr(err) {
		return 0, 0, ErrUserAlreadyExists
	}

	// TODO: Cut additional query here by using RETURNING on PostgreSQL.
	uid, _, _, _, _, err = b.getUserCreds(tx, username)
	if err != nil {
		return 0, 0, wrapErr(err, "CreateUser")
	}

	// Every new user needs to have at least one mailbox (INBOX).
	if _, err := tx.Stmt(b.createMbox).Exec(uid, "INBOX", b.prng.Uint32(), nil); err != nil {
		return 0, 0, wrapErr(err, "CreateUser")
	}

	// Cut another query here by using RETURNING on PostgreSQL.
	if err = tx.Stmt(b.mboxId).QueryRow(uid, "INBOX").Scan(&inboxId); err != nil {
		return 0, 0, wrapErr(err, "CreateUser")
	}
	if _, err = tx.Stmt(b.setInboxId).Exec(inboxId, uid); err != nil {
		return 0, 0, wrapErr(err, "CreateUser")
	}

	if shouldCommit {
		return uid, inboxId, tx.Commit()
	}
	return uid, inboxId, nil
}

// DeleteUser deleted user account with specified username.
//
// It is error to delete account that doesn't exist, ErrUserDoesntExists will
// be returned in this case.
func (b *Backend) DeleteUser(username string) error {
	username = strings.ToLower(username)

	tx, err := b.db.BeginLevel(sql.LevelReadCommitted, false)
	if err != nil {
		return wrapErr(err, "DeleteUser")
	}
	defer tx.Rollback()

	// TODO: These queries definitely can be merged on PostgreSQL.
	var keys []string
	rows, err := tx.Stmt(b.refUser).Query(username)
	if err != nil {
		return wrapErr(err, "DeleteUser")
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return wrapErr(err, "DeleteUser")
		}
		keys = append(keys, key)
	}

	stats, err := tx.Stmt(b.delUser).Exec(username)
	if err != nil {
		return wrapErr(err, "DeleteUser")
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return wrapErr(err, "DeleteUser")
	}
	if affected == 0 {
		return ErrUserDoesntExists
	}

	if err := b.extStore.Delete(keys); err != nil {
		return wrapErr(err, "DeleteUser")
	}

	if _, err := tx.Stmt(b.deleteUserRef).Exec(username); err != nil {
		return wrapErr(err, "DeleteUser")
	}

	return tx.Commit()
}

// ResetPassword sets user account password to invalid value such that Login
// and CheckPlain will always return "invalid credentials" error.
func (b *Backend) ResetPassword(username string) error {
	username = strings.ToLower(username)

	stats, err := b.setUserPass.Exec(nil, nil, username)
	if err != nil {
		return wrapErr(err, "ResetPassword")
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return wrapErr(err, "ResetPassword")
	}
	if affected == 0 {
		return ErrUserDoesntExists
	}
	return nil
}

// SetUserPassword changes password associated with account with specified
// username.
//
// Opts.DefaultHashAlgo is used for password hashing.
// This method can fail if crypto/rand fails to generate enough entropy.
//
// It is error to change password for account that doesn't exist,
// ErrUserDoesntExists will be returned in this case.
func (b *Backend) SetUserPassword(username, newPassword string) error {
	return b.SetUserPasswordWithHash(b.Opts.DefaultHashAlgo, username, newPassword)
}

// SetUserPasswordWithHash is a version of SetUserPassword that allows to
// specify any supported hash algorithm instead of Opts.DefaultHashAlgo.
func (b *Backend) SetUserPasswordWithHash(hashAlgo, username, newPassword string) error {
	username = strings.ToLower(username)

	digest, salt, err := b.hashCredentials(hashAlgo, newPassword)
	if err != nil {
		return err
	}

	stats, err := b.setUserPass.Exec(digest, salt, username)
	if err != nil {
		return wrapErr(err, "SetUserPassword")
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return wrapErr(err, "SetUserPassword")
	}
	if affected == 0 {
		return ErrUserDoesntExists
	}
	return nil
}

// ListUsers returns list of existing usernames.
//
// It may return nil slice if no users are registered.
func (b *Backend) ListUsers() ([]string, error) {
	var res []string
	rows, err := b.listUsers.Query()
	if err != nil {
		return res, wrapErr(err, "ListUsers")
	}
	for rows.Next() {
		var id uint64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return res, wrapErr(err, "ListUsers")
		}
		res = append(res, name)
	}
	if err := rows.Err(); err != nil {
		return res, wrapErr(err, "ListUsers")
	}
	return res, nil
}

// GetUser creates backend.User object without for the user credentials.
//
// If you want to check user credentials, you should use Login or CheckPlain.
func (b *Backend) GetUser(username string) (backend.User, error) {
	username = strings.ToLower(username)

	uid, inboxId, _, _, _, err := b.getUserCreds(nil, username)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrUserDoesntExists
		}
		return nil, err
	}
	return &User{id: uid, username: username, parent: b, inboxId: inboxId}, nil
}

// GetOrCreateUser is a convenience wrapper for GetUser and CreateUser.
//
// Users are created with invalid password such that CheckPlain and Login
// will always return "invalid credentials" error.
//
// All database operations are executed within one transaction so
// this method is atomic as defined by used RDBMS.
func (b *Backend) GetOrCreateUser(username string) (backend.User, error) {
	username = strings.ToLower(username)

	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	uid, inboxId, _, _, _, err := b.getUserCreds(tx, username)
	if err != nil {
		if err == sql.ErrNoRows {
			b.Opts.Log.Println("auto-creating storage account", username)
			if uid, inboxId, err = b.createUser(tx, username, b.Opts.DefaultHashAlgo, nil); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return &User{id: uid, username: username, parent: b, inboxId: inboxId}, tx.Commit()
}

// CheckPlain checks the credentials of the user account.
func (b *Backend) CheckPlain(username, password string) bool {
	_, _, err := b.checkUser(strings.ToLower(username), password)
	return err == nil
}

func (b *Backend) Login(_ *imap.ConnInfo, username, password string) (backend.User, error) {
	uid, inboxId, err := b.checkUser(strings.ToLower(username), password)
	if err != nil {
		return nil, err
	}

	b.Opts.Log.Debugln(username, "logged in")

	return &User{id: uid, username: username, parent: b, inboxId: inboxId}, nil
}

func (b *Backend) CreateMessageLimit() *uint32 {
	return b.Opts.MaxMsgBytes
}

// Change global APPEND limit, Opts.MaxMsgBytes.
//
// Provided to implement interfaces used by go-imap-backend-tests.
func (b *Backend) SetMessageLimit(val *uint32) error {
	b.Opts.MaxMsgBytes = val
	return nil
}
