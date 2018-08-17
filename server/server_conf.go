package server

import (
	. "github.com/siddontang/go-mysql/mysql"
	"fmt"
	"crypto/tls"
	"sync"
)

// supported auth methods
const (
	MYSQL_NATIVE_PASSWORD = "mysql_native_password"
	CACHING_SHA2_PASSWORD = "caching_sha2_password"
	SHA256_PASSWORD       = "sha256_password"
)

var defaultServer = NewDefaultServer()

// Defines a basic MySQL server with configs.
//
// We do not aim at implementing the whole MySQL connection suite to have the best compatibilities for the clients.
// The MySQL server can be configured to switch auth methods covering 'mysql_old_password', 'mysql_native_password',
// 'mysql_clear_password', 'authentication_windows_client', 'sha256_password', 'caching_sha2_password', etc.
//
// However, since some old auth methods are considered broken with security issues. MySQL major versions like 5.7 and 8.0 default to
// 'mysql_native_password' or 'caching_sha2_password', and most MySQL clients should have already supported at least one of the three auth
// methods 'mysql_native_password', 'caching_sha2_password', and 'sha256_password'. Thus here we will only support these three
// auth methods, and use 'mysql_native_password' as default for maximum compatibility with the clients and leave the other two as
// config options.
//
// The MySQL doc states that 'mysql_old_password' will be used if 'CLIENT_PROTOCOL_41' or 'CLIENT_SECURE_CONNECTION' flag is not set.
// We choose to drop the support for insecure 'mysql_old_password' auth method and require client capability 'CLIENT_PROTOCOL_41' and 'CLIENT_SECURE_CONNECTION'
// are set. Besides, if 'CLIENT_PLUGIN_AUTH' is not set, we fallback to 'mysql_native_password' auth method.
type Server struct {
	serverVersion      string // e.g. "8.0.12"
	protocolVersion    int    // minimal 10
	capability         uint32 // server capability flag
	collationId        uint8
	defaultAuthMethod  string   // default authentication method, 'mysql_native_password'
	allowedAuthMethods []string // 'mysql_native_password', 'caching_sha2_password', and 'sha256_password'
	pubKey             []byte
	tlsConfig          *tls.Config
	cacheShaPassword   *sync.Map // 'user@host' -> SHA256(SHA256(PASSWORD))
}

// new mysql server with default settings
func NewDefaultServer() *Server {
	caPem, caKey := generateCA()
	certPem, keyPem := generateAndSignRSACerts(caPem, caKey)
	tlsConf := newServerTLSConfig(certPem, keyPem, tls.RequireAnyClientCert)
	return &Server{
		serverVersion:   "5.7.0",
		protocolVersion: 10,
		capability: CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 |
			CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH,
		collationId:        DEFAULT_COLLATION_ID,
		defaultAuthMethod:  MYSQL_NATIVE_PASSWORD,
		allowedAuthMethods: []string{MYSQL_NATIVE_PASSWORD, CACHING_SHA2_PASSWORD, SHA256_PASSWORD},
		pubKey:             getPublicKeyFromCert(certPem),
		tlsConfig:          tlsConf,
		cacheShaPassword:   new(sync.Map),
	}
}

// new mysql server with customized settings
// the allowedAuthMethods list should include the defaultAuthMethod
func NewServer(serverVersion string, collationId uint8, defaultAuthMethod string, allowedAuthMethods []string, pubKey []byte, tlsConfig *tls.Config) *Server {
	if defaultAuthMethod != MYSQL_NATIVE_PASSWORD && defaultAuthMethod != CACHING_SHA2_PASSWORD && defaultAuthMethod != SHA256_PASSWORD {
		panic(fmt.Sprintf("server authentication method '%s' is not supported", defaultAuthMethod))
	}
	for _, allowed := range allowedAuthMethods {
		if !isAuthMethodSupported(allowed) {
			panic(fmt.Sprintf("server authentication method '%s' is not supported", allowed))
		}
	}
	if !isAuthMethodAllowedByServer(defaultAuthMethod, allowedAuthMethods) {
		panic(fmt.Sprintf("default auth method is not one of the allowed auth methods"))
	}
	return &Server{
		serverVersion:   serverVersion,
		protocolVersion: 10,
		capability: CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 |
			CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH,
		collationId:        collationId,
		defaultAuthMethod:  defaultAuthMethod,
		allowedAuthMethods: allowedAuthMethods,
		pubKey:             pubKey,
		tlsConfig:          tlsConfig,
		cacheShaPassword:   new(sync.Map),
	}
}

func isAuthMethodSupported(authMethod string) bool {
	return authMethod == MYSQL_NATIVE_PASSWORD || authMethod == CACHING_SHA2_PASSWORD || authMethod == SHA256_PASSWORD
}

func isAuthMethodAllowedByServer(authMethod string, allowedAuthMethods []string) bool {
	for _, m := range allowedAuthMethods {
		if m == authMethod {
			return true
		}
	}
	return false
}

func (s * Server) InvalidateCache(username string, host string)  {
	s.cacheShaPassword.Delete(fmt.Sprintf("%s@%s", username, host))
}
