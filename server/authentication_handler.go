package server

import (
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
	// get user credential (supports multiple valid passwords per user)
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

type Credential struct {
	Passwords      []string // raw passwords, hashed on demand during comparison
	AuthPluginName string
}

// HashPassword computes the password hash for a given password using the credential's auth plugin.
func (c Credential) HashPassword(password string) (string, error) {
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

// HasEmptyPassword returns true if any password in the credential is empty.
func (c Credential) HasEmptyPassword() bool {
	for _, p := range c.Passwords {
		if p == "" {
			return true
		}
	}
	return false
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

func (h *InMemoryAuthenticationHandler) OnAuthSuccess(conn *Conn) error {
	return nil
}

func (h *InMemoryAuthenticationHandler) OnAuthFailure(conn *Conn, err error) {
}
