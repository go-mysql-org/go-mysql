package server

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
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
	if authPluginName != c.credential.authPluginName {
		err := c.writeAuthSwitchRequest(c.credential.authPluginName)
		if err != nil {
			return err
		}
		return c.handleAuthSwitchResponse()
	}

	switch authPluginName {
	case mysql.AUTH_NATIVE_PASSWORD:
		return c.compareNativePasswordAuthData(clientAuthData, c.credential)

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		if !c.cachingSha2FullAuth {
			if err := c.compareCacheSha2PasswordAuthData(clientAuthData); err != nil {
				return err
			}
			if c.cachingSha2FullAuth {
				return c.handleAuthSwitchResponse()
			}
			return nil
		}
		// AuthMoreData packet already sent, do full auth
		return c.handleCachingSha2PasswordFullAuth(clientAuthData)

	case mysql.AUTH_SHA256_PASSWORD:
		cont, err := c.handlePublicKeyRetrieval(clientAuthData)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
		return c.compareSha256PasswordAuthData(clientAuthData, c.credential)

	default:
		return errors.Errorf("unknown authentication plugin name '%s'", authPluginName)
	}
}

func (c *Conn) acquirePassword() error {
	credential, found, err := c.credentialProvider.GetCredential(c.user)
	if err != nil {
		return err
	}
	if !found {
		return mysql.NewDefaultError(mysql.ER_NO_SUCH_USER, c.user, c.RemoteAddr().String())
	}
	c.credential = credential
	return nil
}

func errAccessDenied(credential Credential) error {
	if credential.password == "" {
		return ErrAccessDeniedNoPassword
	}

	return ErrAccessDenied
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
	return bytes.Equal(m, cached)
}

func (c *Conn) compareNativePasswordAuthData(clientAuthData []byte, credential Credential) error {
	password, err := mysql.DecodePasswordHex(c.credential.password)
	if err != nil {
		return errAccessDenied(credential)
	}
	if mysql.CompareNativePassword(clientAuthData, password, c.salt) {
		return nil
	}
	return errAccessDenied(credential)
}

func (c *Conn) compareSha256PasswordAuthData(clientAuthData []byte, credential Credential) error {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if credential.password == "" {
			return nil
		}
		return ErrAccessDenied
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
	check, err := mysql.Check256HashingPassword([]byte(credential.password), string(clientAuthData))
	if err != nil {
		return err
	}
	if check {
		return nil
	}
	return ErrAccessDenied
}

func (c *Conn) compareCacheSha2PasswordAuthData(clientAuthData []byte) error {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if c.credential.password == "" {
			return nil
		}
		return ErrAccessDenied
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

		return errAccessDenied(c.credential)
	}
	// cache miss, do full auth
	if err := c.writeAuthMoreDataFullAuth(); err != nil {
		return err
	}
	c.cachingSha2FullAuth = true
	return nil
}
