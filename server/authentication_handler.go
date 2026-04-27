package server

import (
	"slices"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
)

// AuthenticationHandler provides user credentials and authentication lifecycle hooks.
//
// # Important Note
//
// if the password in a third-party auth handler could be updated at runtime, we have to invalidate the caching
// for 'caching_sha2_password' by calling 'func (s *Server)InvalidateCache(string, string)'.
type AuthenticationHandler interface {
	// GetCredential returns the user credential (supports multiple valid passwords per user).
	// Implementations must be safe for concurrent use.
	GetCredential(username string) (credential Credential, found bool, err error)

	// OnAuthSuccess is called after successful authentication, before the OK packet.
	// Return an error to reject the connection (error will be sent to client instead of OK).
	// Return nil to proceed with sending the OK packet.
	OnAuthSuccess(conn *Conn) error

	// OnAuthFailure is called after authentication fails, before the error packet.
	// This is informational only - the connection will be closed regardless.
	OnAuthFailure(conn *Conn, err error)
}

func NewInMemoryAuthenticationHandler(defaultAuthMethod ...string) *InMemoryAuthenticationHandler {
	d := mysql.AUTH_CACHING_SHA2_PASSWORD
	if len(defaultAuthMethod) > 0 {
		d = defaultAuthMethod[0]
	}
	return &InMemoryAuthenticationHandler{
		userPool:          sync.Map{},
		defaultAuthMethod: d,
	}
}

// Credential holds authentication settings for a user.
//
// Passwords contains all valid raw passwords for the user. They are hashed on demand during comparison.
// If empty password authentication is allowed, Passwords must contain an empty string (e.g., []string{""})
// rather than being a zero-length slice. A zero-length slice means no valid passwords are configured.
//
// HashedPasswords contains pre-computed password hashes that the server compares directly against the
// client's challenge response, without ever needing the plaintext. This lets callers (e.g. a MySQL proxy
// rehoming users from another server's `mysql.user` table) configure credentials when only the stored
// hash is available. The byte format depends on AuthPluginName:
//
//   - mysql_native_password: the 20-byte SHA1(SHA1(plaintext)) value, i.e. what NativePasswordHash
//     returns, or what DecodePasswordHex returns from MySQL's standard "*XXXX..." (41-char) hex form.
//
// Other auth plugins do not currently consume HashedPasswords; for them, populate Passwords instead.
//
// Both fields can be set on the same Credential: HashedPasswords is checked first (cheaper, no
// hashing per connect), then Passwords.
type Credential struct {
	Passwords       []string
	HashedPasswords [][]byte
	AuthPluginName  string
}

// hashPassword computes the password hash for a given password using the credential's auth plugin.
func (c Credential) hashPassword(password string) (string, error) {
	if password == "" {
		return "", nil
	}

	switch c.AuthPluginName {
	case mysql.AUTH_NATIVE_PASSWORD:
		return mysql.EncodePasswordHex(mysql.NativePasswordHash([]byte(password))), nil

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		return auth.NewHashPassword(password, mysql.AUTH_CACHING_SHA2_PASSWORD), nil

	case mysql.AUTH_SHA256_PASSWORD:
		return mysql.NewSha256PasswordHash(password)

	case mysql.AUTH_CLEAR_PASSWORD:
		return password, nil

	default:
		return "", errors.Errorf("unknown authentication plugin name '%s'", c.AuthPluginName)
	}
}

// hasEmptyPassword returns true if any password in the credential is empty.
func (c Credential) hasEmptyPassword() bool {
	return slices.Contains(c.Passwords, "")
}

// hasAnyCredential reports whether the credential has at least one usable
// entry — either a plaintext password (including the empty string, which
// signals "empty password is OK") or a pre-computed hash.
func (c Credential) hasAnyCredential() bool {
	return len(c.Passwords) > 0 || len(c.HashedPasswords) > 0
}

// InMemoryAuthenticationHandler implements AuthenticationHandler with in-memory credential storage.
type InMemoryAuthenticationHandler struct {
	userPool          sync.Map // username -> Credential
	defaultAuthMethod string
}

func (h *InMemoryAuthenticationHandler) CheckUsername(username string) (found bool, err error) {
	_, ok := h.userPool.Load(username)
	return ok, nil
}

func (h *InMemoryAuthenticationHandler) GetCredential(username string) (credential Credential, found bool, err error) {
	v, ok := h.userPool.Load(username)
	if !ok {
		return Credential{}, false, nil
	}
	c, valid := v.(Credential)
	if !valid {
		return Credential{}, true, errors.Errorf("invalid credential")
	}
	return c, true, nil
}

func (h *InMemoryAuthenticationHandler) AddUser(username, password string, optionalAuthPluginName ...string) error {
	authPluginName := h.defaultAuthMethod
	if len(optionalAuthPluginName) > 0 {
		authPluginName = optionalAuthPluginName[0]
	}

	if !isAuthMethodSupported(authPluginName) {
		return errors.Errorf("unknown authentication plugin name '%s'", authPluginName)
	}

	h.userPool.Store(username, Credential{
		Passwords:      []string{password},
		AuthPluginName: authPluginName,
	})
	return nil
}

// AddUserWithHashedPassword registers a user whose password is already in
// the server-side hashed form, so the plaintext never has to be supplied.
//
// The expected byte format depends on authPluginName; see Credential.HashedPasswords.
// For mysql_native_password (currently the only plugin that consumes
// HashedPasswords) hash must be the 20-byte SHA1(SHA1(plaintext)) value.
// To take MySQL's standard 41-character "*XXXX..." form, use
// mysql.DecodePasswordHex first:
//
//	bytes, _ := mysql.DecodePasswordHex("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9")
//	handler.AddUserWithHashedPassword("alice", bytes)
//
// authPluginName defaults to the handler's default auth method.
func (h *InMemoryAuthenticationHandler) AddUserWithHashedPassword(username string, hash []byte, optionalAuthPluginName ...string) error {
	authPluginName := h.defaultAuthMethod
	if len(optionalAuthPluginName) > 0 {
		authPluginName = optionalAuthPluginName[0]
	}

	if !isAuthMethodSupported(authPluginName) {
		return errors.Errorf("unknown authentication plugin name '%s'", authPluginName)
	}
	if authPluginName != mysql.AUTH_NATIVE_PASSWORD {
		// Future work: extend HashedPasswords semantics to caching_sha2_password
		// and sha256_password. Reject up front so callers don't silently get
		// "auth always fails" for unsupported plugins.
		return errors.Errorf("AddUserWithHashedPassword does not yet support auth plugin '%s'; only '%s' is supported", authPluginName, mysql.AUTH_NATIVE_PASSWORD)
	}
	if len(hash) == 0 {
		return errors.New("hashed password must not be empty")
	}

	h.userPool.Store(username, Credential{
		HashedPasswords: [][]byte{hash},
		AuthPluginName:  authPluginName,
	})
	return nil
}

func (h *InMemoryAuthenticationHandler) OnAuthSuccess(conn *Conn) error {
	return nil
}

func (h *InMemoryAuthenticationHandler) OnAuthFailure(conn *Conn, err error) {
}
