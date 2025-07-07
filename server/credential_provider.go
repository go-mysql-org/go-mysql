package server

import (
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
)

// interface for user credential provider
// hint: can be extended for more functionality
//
// # Important Note
//
// if the password in a third-party credential provider could be updated at runtime, we have to invalidate the caching
// for 'caching_sha2_password' by calling 'func (s *Server)InvalidateCache(string, string)'.
type CredentialProvider interface {
	// check if the user exists
	CheckUsername(username string) (bool, error)
	// get user credential
	GetCredential(username string) (credential Credential, found bool, err error)
}

func NewInMemoryProvider(defaultAuthMethod ...string) *InMemoryProvider {
	d := mysql.AUTH_CACHING_SHA2_PASSWORD
	if len(defaultAuthMethod) > 0 {
		d = defaultAuthMethod[0]
	}
	return &InMemoryProvider{
		userPool:          sync.Map{},
		defaultAuthMethod: d,
	}
}

type Credential struct {
	Password       string
	AuthPluginName string
}

func NewCredential(password string, authPluginName string) (Credential, error) {
	c := Credential{
		AuthPluginName: authPluginName,
	}

	if password == "" {
		c.Password = ""
		return c, nil
	}

	switch c.AuthPluginName {
	case mysql.AUTH_NATIVE_PASSWORD:
		c.Password = mysql.EncodePasswordHex(mysql.NativePasswordHash([]byte(password)))

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		c.Password = auth.NewHashPassword(password, mysql.AUTH_CACHING_SHA2_PASSWORD)

	case mysql.AUTH_SHA256_PASSWORD:
		hash, err := mysql.NewSha256PasswordHash(password)
		if err != nil {
			return c, err
		}
		c.Password = hash

	case mysql.AUTH_CLEAR_PASSWORD:
		c.Password = password

	default:
		return c, errors.Errorf("unknown authentication plugin name '%s'", c.AuthPluginName)
	}
	return c, nil
}

// implements an in memory credential provider
type InMemoryProvider struct {
	userPool          sync.Map // username -> password
	defaultAuthMethod string
}

func (m *InMemoryProvider) CheckUsername(username string) (found bool, err error) {
	_, ok := m.userPool.Load(username)
	return ok, nil
}

func (m *InMemoryProvider) GetCredential(username string) (credential Credential, found bool, err error) {
	v, ok := m.userPool.Load(username)
	if !ok {
		return Credential{}, false, nil
	}
	c, valid := v.(Credential)
	if !valid {
		return Credential{}, true, errors.Errorf("invalid credential")
	}
	return c, true, nil
}

func (m *InMemoryProvider) AddUser(username, password string, optionalAuthPluginName ...string) error {
	authPluginName := m.defaultAuthMethod
	if len(optionalAuthPluginName) > 0 {
		authPluginName = optionalAuthPluginName[0]
	}

	c, err := NewCredential(password, authPluginName)
	if err != nil {
		return err
	}

	m.userPool.Store(username, c)
	return nil
}

type Provider InMemoryProvider
