package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

var (
	ErrAccessDenied           = errors.New("access denied")
	ErrAccessDeniedNoPassword = fmt.Errorf("%w without password", ErrAccessDenied)
)

// isEmptyPassword returns true if the auth data represents an empty password.
// Some clients send an empty packet (len == 0), while others (e.g. MySQL's libmysql)
// send a single null byte. This matches MySQL server's own handling:
// if (!pkt_len || (pkt_len == 1 && *pkt == 0))
// https://github.com/mysql/mysql-server/blob/8.0/sql/auth/sha2_password.cc
// https://github.com/mysql/mysql-server/blob/8.0/sql/auth/sql_authentication.cc
func isEmptyPassword(authData []byte) bool {
	return len(authData) == 0 || (len(authData) == 1 && authData[0] == 0)
}

func (c *Conn) compareAuthData(authPluginName string, clientAuthData []byte) error {
	if authPluginName != c.credential.AuthPluginName {
		err := c.writeAuthSwitchRequest(c.credential.AuthPluginName)
		if err != nil {
			return err
		}
		return c.handleAuthSwitchResponse()
	}

	return c.serverConf.authProvider.Authenticate(c, authPluginName, clientAuthData)
}

func (c *Conn) acquireCredential() error {
	if c.credential.hasAnyCredential() {
		return nil
	}
	credential, found, err := c.authHandler.GetCredential(c.user)
	if err != nil {
		return err
	}
	if !found || !credential.hasAnyCredential() {
		return mysql.NewDefaultError(mysql.ER_NO_SUCH_USER, c.user, c.RemoteAddr().String())
	}
	c.credential = credential
	return nil
}

func scrambleValidation(cached, nonce, scramble []byte) bool {
	// SHA256(SHA256(SHA256(STORED_PASSWORD)), NONCE)
	crypt := sha256.New()
	crypt.Write(cached)
	crypt.Write(nonce)
	message2 := crypt.Sum(nil)
	// SHA256(PASSWORD)
	if len(message2) != len(scramble) {
		return false
	}
	for i := range message2 {
		message2[i] ^= scramble[i]
	}
	// SHA256(SHA256(PASSWORD)
	crypt.Reset()
	crypt.Write(message2)
	m := crypt.Sum(nil)
	return subtle.ConstantTimeCompare(m, cached) == 1
}

func (c *Conn) compareNativePasswordAuthData(clientAuthData []byte, credential Credential) error {
	if isEmptyPassword(clientAuthData) {
		if credential.hasEmptyPassword() {
			return nil
		}
		return ErrAccessDeniedNoPassword
	}

	// Pre-computed hashes are checked first: they let callers configure
	// credentials when only the server-side hash is available (no plaintext),
	// and they're cheaper per connect because we skip the SHA1(SHA1(...)) step.
	for _, hp := range credential.HashedPasswords {
		if mysql.CompareNativePassword(clientAuthData, hp.data, c.salt) {
			return nil
		}
	}

	for _, password := range credential.Passwords {
		hash, err := credential.hashPassword(password)
		if err != nil {
			continue
		}
		decoded, err := mysql.DecodePasswordHex(hash)
		if err != nil {
			continue
		}
		if mysql.CompareNativePassword(clientAuthData, decoded, c.salt) {
			return nil
		}
	}
	return ErrAccessDenied
}

func (c *Conn) compareSha256PasswordAuthData(clientAuthData []byte, credential Credential) error {
	if isEmptyPassword(clientAuthData) {
		if credential.hasEmptyPassword() {
			return nil
		}
		return ErrAccessDeniedNoPassword
	}
	if tlsConn, ok := c.Conn.Conn.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			return errors.New("incomplete TSL handshake")
		}
		// connection is SSL/TLS, client should send plain password
		// deal with the trailing \NUL added for plain text password received
		if l := len(clientAuthData); l != 0 && clientAuthData[l-1] == 0x00 {
			clientAuthData = clientAuthData[:l-1]
		}
	} else {
		// client should send encrypted password
		// decrypt
		if c.serverConf.rsaPrivateKey == nil {
			return errors.New("RSA key not configured; non-TLS connections are not supported for this authentication method")
		}
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, c.serverConf.rsaPrivateKey, clientAuthData, nil)
		if err != nil {
			return err
		}
		clientAuthData = mysql.Xor(dbytes, c.salt)
		if l := len(clientAuthData); l != 0 && clientAuthData[l-1] == 0x00 {
			clientAuthData = clientAuthData[:l-1]
		}
	}
	// Pre-computed hashes are checked first: callers can configure
	// credentials when only the server-side stored hash is available
	// (e.g. mirroring `mysql.user.authentication_string`), and we skip
	// the per-connect hashPassword work.
	for _, hp := range credential.HashedPasswords {
		check, err := mysql.Check256HashingPassword(hp.data, string(clientAuthData))
		if err != nil {
			// Stored hashes are shape-checked at construction time
			// (validateHashedPassword via NewHashedPassword), so reaching
			// this branch implies the upstream verifier changed format
			// expectations. Log via the configurable logger so the silent
			// skip is auditable; auth still falls through to the plaintext
			// loop and ultimately ErrAccessDenied if nothing matches.
			c.serverConf.logger().Error("sha256_password hash compare error", "user", c.user, "error", err)
			continue
		}
		if check {
			return nil
		}
	}

	for _, password := range credential.Passwords {
		hash, err := credential.hashPassword(password)
		if err != nil {
			continue
		}
		check, err := mysql.Check256HashingPassword([]byte(hash), string(clientAuthData))
		if err != nil {
			continue
		}
		if check {
			return nil
		}
	}
	return ErrAccessDenied
}

func (c *Conn) compareCacheSha2PasswordAuthData(clientAuthData []byte) error {
	if isEmptyPassword(clientAuthData) {
		if c.credential.hasEmptyPassword() {
			return nil
		}
		return ErrAccessDeniedNoPassword
	}
	// the caching of 'caching_sha2_password' in MySQL, see: https://dev.mysql.com/worklog/task/?id=9591
	// check if we have a cached value
	cached, ok := c.serverConf.cacheShaPassword.Load(fmt.Sprintf("%s@%s", c.user, c.LocalAddr()))
	if ok {
		// Scramble validation
		if scrambleValidation(cached.([]byte), c.salt, clientAuthData) {
			// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
			return c.writeAuthMoreDataFastAuth()
		}
	}
	// cache miss or validation failed, do full auth
	if err := c.writeAuthMoreDataFullAuth(); err != nil {
		return err
	}
	c.cachingSha2FullAuth = true
	return nil
}
