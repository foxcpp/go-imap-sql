package imapsql

import (
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"

	"errors"

	"github.com/emersion/go-imap/backend"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/sha3"
)

type hashAlgorithm struct {
	hashFunc  func([]byte) []byte
	checkFunc func([]byte, []byte) bool
}

func (b *Backend) enableDefaultHashAlgs() {
	b.EnableHashAlgo("sha3-512", func(pass []byte) []byte {
		digest := sha3.Sum512(pass)
		encoded := make([]byte, hex.EncodedLen(len(digest[:])))
		hex.Encode(encoded, digest[:])
		return encoded
	}, func(pass, hash []byte) bool {
		rawDigest := make([]byte, hex.DecodedLen(len(hash)))
		_, err := hex.Decode(rawDigest, hash)
		if err != nil {
			return false
		}
		digest := sha3.Sum512(pass)
		return subtle.ConstantTimeCompare(digest[:], rawDigest) == 1
	})
	b.EnableHashAlgo("bcrypt", func(pass []byte) []byte {
		digest, _ := bcrypt.GenerateFromPassword(pass, b.Opts.BcryptCost)
		return digest[:]
	}, func(pass, hash []byte) bool {
		return bcrypt.CompareHashAndPassword(hash, pass) == nil
	})
}

// EnableHashAlgo enables support for additional hash algorithm using two
// provided callback functions.
//
// Note that generated hash should be ASCII-encoded.
func (b *Backend) EnableHashAlgo(name string,
	hashFunc func(saltedPass []byte) []byte,
	checkFunc func(saltedPass, hash []byte) bool) {
	b.hashAlgorithms[name] = hashAlgorithm{hashFunc, checkFunc}
}

func (b *Backend) checkUser(username, password string) (uid uint64, inboxId uint64, err error) {
	uid, inboxId, hashAlgo, passHash, passSalt, err := b.getUserCreds(nil, username)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, backend.ErrInvalidCredentials
		}
		return 0, 0, err
	}

	algoFuncs, ok := b.hashAlgorithms[hashAlgo]
	if !ok {
		return 0, 0, errors.New("unsupported hash algorithm")
	}

	if passHash == nil || passSalt == nil {
		return uid, 0, backend.ErrInvalidCredentials
	}

	pass := make([]byte, 0, len(password)+len(passSalt))
	pass = append(pass, []byte(password)...)
	pass = append(pass, passSalt...)
	if !algoFuncs.checkFunc(pass, passHash) {
		return uid, 0, backend.ErrInvalidCredentials
	}

	return uid, inboxId, nil
}

func (b *Backend) hashCredentials(algo, password string) (digest, salt string, err error) {
	saltBytes := make([]byte, 16)
	if n, err := rand.Read(saltBytes); err != nil {
		return "", "", err
	} else if n != 16 {
		return "", "", errors.New("failed to read enough entropy for salt from CSPRNG")
	}

	algoFuncs, ok := b.hashAlgorithms[algo]
	if !ok {
		return "", "", errors.New("unsupported hash algorithm")
	}

	pass := make([]byte, 0, len(password)+len(saltBytes))
	pass = append(pass, []byte(password)...)
	pass = append(pass, saltBytes...)
	digestBytes := algoFuncs.hashFunc(pass)
	digest = algo + ":" + string(digestBytes)
	salt = hex.EncodeToString(saltBytes)

	return digest, salt, nil
}
