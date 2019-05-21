package imapsql

import (
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	mathrand "math/rand"
	"strings"
	"sync"
	"time"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/pkg/errors"
	"golang.org/x/crypto/sha3"
)

var (
	ErrUserAlreadyExists = errors.New("imap: user already exists")
	ErrUserDoesntExists  = errors.New("imap: user doesn't exists")
)

type Rand interface {
	Uint32() uint32
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

	// External storage to use to store message bodies. If specified - all new messages
	// will be saved to it. However, already existing messages stored in DB
	// directly will not be moved.
	ExternalStore ExternalStore

	// Automatically update database schema on imapsql.New.
	AllowSchemaUpgrade bool
}

type Backend struct {
	db db

	// Opts structure used to construct this Backend object.
	//
	// For most cases it is safe to change options while backend is serving
	// requests.
	// Options that should NOT be changed while backend is processing commands:
	// - ExternalStore
	// - PRNG
	// Changes for the following options have no effect after backend initialization:
	// - AllowSchemaUpgrade
	// - ExclusiveLock
	// - CacheSize
	// - NoWAL
	// - UpdatesChan
	Opts Opts

	// database/sql.DB object created by New.
	DB *sql.DB

	childrenExt bool

	prng Rand

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
	getMboxMark        *sql.Stmt
	setSubbed          *sql.Stmt
	uidNext            *sql.Stmt
	addUidNext         *sql.Stmt
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

	searchFetchNoBody      *sql.Stmt
	searchFetch            *sql.Stmt
	searchFetchNoBodyNoSeq *sql.Stmt
	searchFetchNoSeq       *sql.Stmt

	flagsSearchStmtsLck   sync.RWMutex
	flagsSearchStmtsCache map[string]*sql.Stmt
	fetchStmtsLck         sync.RWMutex
	fetchStmtsCache       map[string]*sql.Stmt

	// extkeys table
	addExtKey             *sql.Stmt
	decreaseRefForMarked  *sql.Stmt
	decreaseRefForDeleted *sql.Stmt
	incrementRefUid       *sql.Stmt
	incrementRefSeq       *sql.Stmt
	zeroRef               *sql.Stmt
	deleteZeroRef         *sql.Stmt
}

// New creates new Backend instance using provided configuration.
//
// driver and dsn arguments are passed directly to sql.Open.
//
// Note that it is not safe to create multiple Backend instances working with
// the single database as they need to keep some state synchronized and there
// is no measures for this implemented in go-imap-sql.
func New(driver, dsn string, opts Opts) (*Backend, error) {
	b := &Backend{
		fetchStmtsCache:       make(map[string]*sql.Stmt),
		flagsSearchStmtsCache: make(map[string]*sql.Stmt),
	}
	var err error

	b.Opts = opts
	if !b.Opts.LazyUpdatesInit {
		b.updates = b.Opts.UpdatesChan
		if b.updates == nil {
			b.updates = make(chan backend.Update, 20)
		}
	}

	if b.Opts.PRNG != nil {
		b.prng = opts.PRNG
	} else {
		b.prng = mathrand.New(mathrand.NewSource(time.Now().Unix()))
	}

	b.db.driver = driver
	b.db.dsn = dsn

	b.db.DB, err = sql.Open(driver, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}
	b.DB = b.db.DB

	ver, err := b.schemaVersion()
	if err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}
	// Zero version indicates "empty database".
	if ver > SchemaVersion {
		return nil, errors.Errorf("incompatible database schema, too new (%d > %d)", ver, SchemaVersion)
	}
	if ver < SchemaVersion && ver != 0 {
		if !opts.AllowSchemaUpgrade {
			return nil, errors.Errorf("incompatible database schema, upgrade required (%d < %d)", ver, SchemaVersion)
		}
		if err := b.upgradeSchema(ver); err != nil {
			return nil, errors.Wrap(err, "NewBackend")
		}
	}
	if err := b.setSchemaVersion(SchemaVersion); err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}

	if err := b.configureEngine(); err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}

	if err := b.initSchema(); err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}
	if err := b.prepareStmts(); err != nil {
		return nil, errors.Wrap(err, "NewBackend")
	}

	for _, item := range [...]imap.FetchItem{
		imap.FetchFlags, imap.FetchEnvelope,
		imap.FetchBodyStructure, "BODY[]", "BODY[HEADER.FIELDS (From To)]"} {

		if _, err := b.getFetchStmt(true, []imap.FetchItem{item}); err != nil {
			return nil, errors.Wrapf(err, "fetchStmt prime (%s, uid=true)", item)
		}
		if _, err := b.getFetchStmt(false, []imap.FetchItem{item}); err != nil {
			return nil, errors.Wrapf(err, "fetchStmt prime (%s, uid=false)", item)
		}
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

func (b *Backend) Close() error {
	if b.db.driver == "sqlite3" {
		// These operations are not critical, so it's not a problem if they fail.
		if b.Opts.MinimizeOnClose {
			b.db.Exec(`VACUUM`)
			b.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
		}

		b.db.Exec(`PRAGMA optimize`)
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

// UserCreds returns internal identifier and credentials for user named
// username.
func (b *Backend) UserCreds(username string) (id uint64, passHash []byte, passSalt []byte, err error) {
	return b.getUserCreds(nil, username)
}

func (b *Backend) getUserCreds(tx *sql.Tx, username string) (id uint64, passHash []byte, passSalt []byte, err error) {
	var row *sql.Row
	if tx != nil {
		row = tx.Stmt(b.userCreds).QueryRow(username)
	} else {
		row = b.userCreds.QueryRow(username)
	}
	var passHashHex, passSaltHex sql.NullString
	if err := row.Scan(&id, &passHashHex, &passSaltHex); err != nil {
		return 0, nil, nil, err
	}

	if !passHashHex.Valid || !passSaltHex.Valid {
		return id, nil, nil, nil
	}

	passHash, err = hex.DecodeString(passHashHex.String)
	if err != nil {
		return 0, nil, nil, err
	}
	passSalt, err = hex.DecodeString(passSaltHex.String)
	if err != nil {
		return 0, nil, nil, err
	}

	return id, passHash, passSalt, nil
}

// CreateUser creates user account with specified credentials.
//
// This method can fail if used crypto/rand fails to create enough entropy.
// It is error to create account with username that already exists.
// ErrUserAlreadyExists will be returned in this case.
func (b *Backend) CreateUser(username, password string) error {
	return b.createUser(nil, username, &password)
}

// CreateUserNoPass creates new user account without a password set.
//
// It will be unable to log in until SetUserPassword is called for it.
func (b *Backend) CreateUserNoPass(username string) error {
	return b.createUser(nil, username, nil)
}

func (b *Backend) createUser(tx *sql.Tx, username string, password *string) error {
	var passHash, passSalt sql.NullString
	if password != nil {
		salt := make([]byte, 16)
		if n, err := rand.Read(salt); err != nil {
			return errors.Wrap(err, "CreateUser")
		} else if n != 16 {
			return errors.New("CreateUser: failed to read enough entropy for salt from CSPRNG")
		}

		pass := make([]byte, 0, len(*password)+len(salt))
		pass = append(pass, []byte(*password)...)
		pass = append(pass, salt...)
		digest := sha3.Sum512(pass)

		passHash.Valid = true
		passHash.String = hex.EncodeToString(digest[:])
		passSalt.Valid = true
		passSalt.String = hex.EncodeToString(salt)
	}

	var err error
	if tx != nil {
		_, err = tx.Stmt(b.addUser).Exec(username, passHash, passSalt)
	} else {
		_, err = b.addUser.Exec(username, passHash, passSalt)
	}
	if err != nil && (strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "unique")) {
		return ErrUserAlreadyExists
	}

	return errors.Wrap(err, "CreateUser")
}

// DeleteUser deleted user account with specified username.
//
// It is error to delete account that doesn't exist, ErrUserDoesntExists will
// be returned in this case.
func (b *Backend) DeleteUser(username string) error {
	stats, err := b.delUser.Exec(username)
	if err != nil {
		return errors.Wrap(err, "DeleteUser")
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "DeleteUser")
	}

	if affected == 0 {
		return ErrUserDoesntExists
	}
	return nil
}

// ResetPassword sets user account password to invalid value such that Login
// and CheckPlain will always return "invalid credentials" error.
func (b *Backend) ResetPassword(username string) error {
	stats, err := b.setUserPass.Exec(nil, nil, username)
	if err != nil {
		return errors.Wrap(err, "ResetPassword")
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "ResetPassword")
	}
	if affected == 0 {
		return ErrUserDoesntExists
	}
	return nil
}

// SetUserPassword changes password associated with account with specified
// username.
//
// This method can fail if crypto/rand fails to generate enough entropy.
//
// It is error to change password for account that doesn't exist,
// ErrUserDoesntExists will be returned in this case.
func (b *Backend) SetUserPassword(username, newPassword string) error {
	salt := make([]byte, 16)
	if n, err := rand.Read(salt); err != nil {
		return errors.Wrap(err, "SetUserPassword")
	} else if n != 16 {
		return errors.New("SetUserPassword: failed to read enough entropy for salt from CSPRNG")
	}

	pass := make([]byte, 0, len(newPassword)+len(salt))
	pass = append(pass, []byte(newPassword)...)
	pass = append(pass, salt...)
	digest := sha3.Sum512(pass)

	stats, err := b.setUserPass.Exec(hex.EncodeToString(digest[:]), hex.EncodeToString(salt), username)
	if err != nil {
		return errors.Wrap(err, "SetUserPassword")
	}
	affected, err := stats.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "SetUserPassword")
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
		return res, errors.Wrap(err, "ListUsers")
	}
	for rows.Next() {
		var id uint64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return res, errors.Wrap(err, "ListUsers")
		}
		res = append(res, name)
	}
	if err := rows.Err(); err != nil {
		return res, errors.Wrap(err, "ListUsers")
	}
	return res, nil
}

// GetUser creates backend.User object without for the user credentials.
//
// If you want to check user credentials, you should use Login or CheckPlain.
func (b *Backend) GetUser(username string) (backend.User, error) {
	uid, _, _, err := b.UserCreds(username)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrUserDoesntExists
		}
		return nil, err
	}
	return &User{id: uid, username: username, parent: b}, nil
}

// GetOrCreateUser is a convenience wrapper for GetUser and CreateUser.
//
// Users are created with invalid password such that CheckPlain and Login
// will always return "invalid credentials" error.
//
// All database operations are executed within one transaction so
// this method is atomic as defined by used RDBMS.
func (b *Backend) GetOrCreateUser(username string) (backend.User, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	uid, _, _, err := b.getUserCreds(tx, username)
	if err != nil {
		if err == sql.ErrNoRows {
			if err := b.createUser(tx, username, nil); err != nil {
				return nil, err
			}
			uid, _, _, err = b.getUserCreds(tx, username)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return &User{id: uid, username: username, parent: b}, tx.Commit()
}

func (b *Backend) checkUser(username, password string) (uint64, error) {
	uid, passHash, passSalt, err := b.getUserCreds(nil, username)
	if err != nil {
		return 0, backend.ErrInvalidCredentials
	}

	if passHash == nil || passSalt == nil {
		return uid, backend.ErrInvalidCredentials
	}

	pass := make([]byte, 0, len(password)+len(passSalt))
	pass = append(pass, []byte(password)...)
	pass = append(pass, passSalt...)
	digest := sha3.Sum512(pass)
	if subtle.ConstantTimeCompare(digest[:], passHash) != 1 {
		return uid, backend.ErrInvalidCredentials
	}

	return uid, nil
}

// CheckPlain checks the credentials of the user account.
func (b *Backend) CheckPlain(username, password string) bool {
	_, err := b.checkUser(username, password)
	return err == nil
}

func (b *Backend) Login(_ *imap.ConnInfo, username, password string) (backend.User, error) {
	uid, err := b.checkUser(username, password)
	if err != nil {
		return nil, err
	}

	return &User{id: uid, username: username, parent: b}, nil
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
