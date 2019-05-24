package imapsql

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"

	"github.com/emersion/go-imap/backend"
	"github.com/pkg/errors"
	"golang.org/x/crypto/sha3"
)

func (b *Backend) checkUser(username, password string) (uint64, error) {
	uid, hashAlgo, passHash, passSalt, err := b.getUserCreds(nil, username)
	if err != nil {
		return 0, backend.ErrInvalidCredentials
	}

	if hashAlgo != "sha3-512" {
		return 0, errors.New("unsupported hash algorithm")
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

func (b *Backend) hashCredentials(algo, password string) (digest, salt string, err error) {
	saltBytes := make([]byte, 16)
	if n, err := rand.Read(saltBytes); err != nil {
		return "", "", errors.WithStack(err)
	} else if n != 16 {
		return "", "", errors.New("failed to read enough entropy for salt from CSPRNG")
	}

	if algo != "sha3-512" {
		return "", "", errors.New("unsupported hash algorithm")
	}

	pass := make([]byte, 0, len(password)+len(saltBytes))
	pass = append(pass, []byte(password)...)
	pass = append(pass, saltBytes...)
	digestBytes := sha3.Sum512(pass)
	digest = "sha3-512:" + hex.EncodeToString(digestBytes[:])
	salt = hex.EncodeToString(saltBytes)

	return digest, salt, nil
}
