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
	if len(c.credential.Passwords) > 0 {
		return nil
	}
	credential, found, err := c.authHandler.GetCredential(c.user)
	if err != nil {
		return err
	}
	if !found || len(credential.Passwords) == 0 {
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
	if len(clientAuthData) == 0 {
		if credential.HasEmptyPassword() {
			return nil
		}
		return ErrAccessDeniedNoPassword
	}

	for _, password := range credential.Passwords {
		hash, err := credential.HashPassword(password)
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
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if credential.HasEmptyPassword() {
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
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.serverConf.tlsConfig.Certificates[0].PrivateKey).(*rsa.PrivateKey), clientAuthData, nil)
		if err != nil {
			return err
		}
		clientAuthData = mysql.Xor(dbytes, c.salt)
		if l := len(clientAuthData); l != 0 && clientAuthData[l-1] == 0x00 {
			clientAuthData = clientAuthData[:l-1]
		}
	}
	for _, password := range credential.Passwords {
		hash, err := credential.HashPassword(password)
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
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if c.credential.HasEmptyPassword() {
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

		return ErrAccessDenied
	}
	// cache miss, do full auth
	if err := c.writeAuthMoreDataFullAuth(); err != nil {
		return err
	}
	c.cachingSha2FullAuth = true
	return nil
}
