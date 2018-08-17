package server

import (
	"bytes"
	. "github.com/siddontang/go-mysql/mysql"
	"github.com/juju/errors"
	"crypto/rsa"
	"crypto/rand"
	"crypto/sha1"
	"github.com/siddontang/go-log/log"
	"crypto/tls"
	"fmt"
	"crypto/sha256"
)

func (c *Conn) compareAuthData(authPluginName string, clientAuthData []byte) (bool, error) {
	errAccessDenied := NewDefaultError(ER_ACCESS_DENIED_ERROR, c.LocalAddr().String(), c.user, "Yes")
	switch authPluginName {
	case MYSQL_NATIVE_PASSWORD:
		if err := c.acquirePassword(); err != nil {
			return false, err
		}
		if bytes.Equal(CalcPassword(c.salt, []byte(c.password)), clientAuthData) {
			return false, nil
		}
		return false, errAccessDenied

	case CACHING_SHA2_PASSWORD:
		return c.compareCacheSha2PasswordAuthData(clientAuthData)

	case SHA256_PASSWORD:
		if err := c.acquirePassword(); err != nil {
			return false, err
		}
		ok, err := c.compareSha256PasswordAuthData(clientAuthData, c.password)
		if err != nil {
			return false, err
		}
		if ok {
			return false, nil
		}
		return false, errAccessDenied

	default:
		return false, errors.Errorf("unknown authentication plugin name")
	}
}

func (c *Conn) acquirePassword() error {
	password, found, err := c.credentialProvider.GetCredential(c.user)
	if err != nil {
		return err
	}
	if !found {
		return NewDefaultError(ER_NO_SUCH_USER, c.user, c.RemoteAddr().String())
	}
	c.password = password
	return nil
}

func scrambleValidation(cached, nonce, scramble []byte) bool {
	// SHA256(SHA256(SHA256(STORED_PASSWORD)), NONCE
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

func (c *Conn) compareSha256PasswordAuthData(clientAuthData []byte, password string) (bool, error) {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 && password == "" {
		return true, nil
	}
	if tlsConn, ok := c.Conn.Conn.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			return false, errors.New("incomplete TSL handshake")
		}
		// connection is SSL/TLS, client should send plain password
		return bytes.Equal(clientAuthData, []byte(password)), nil
	} else {
		// client should send encrypted password
		// decrypt
		log.Debug("compareAuthSwitchData: decrypt a SHA256_PASSWORD")
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.serverConf.tlsConfig.Certificates[0].PrivateKey).(*rsa.PrivateKey), clientAuthData, nil)
		if err != nil {
			return false, err
		}
		plain := make([]byte, len(password)+1)
		copy(plain, password)
		for i := range plain {
			j := i % len(c.salt)
			plain[i] ^= c.salt[j]
		}
		return bytes.Equal(plain, dbytes), nil
	}
}

func (c *Conn) compareCacheSha2PasswordAuthData(clientAuthData []byte) (bool, error)  {
	errAccessDenied := NewDefaultError(ER_ACCESS_DENIED_ERROR, c.LocalAddr().String(), c.user, "Yes")
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if err := c.acquirePassword(); err != nil {
			return false, err
		}
		if c.password == "" {
			return false, nil
		}
		return false, errAccessDenied
	}
	// the caching of 'caching_sha2_password' in MySQL, see: https://dev.mysql.com/worklog/task/?id=9591
	if _, ok := c.credentialProvider.(*InMemoryProvider); ok {
		// since we have already kept the password in memory and calculate the scramble is not that high of cost, we eliminate
		// the caching part. So our server will never ask the client to do a full authentication via RSA key exchange and it appears
		// like the auth will always hit the cache.
		if err := c.acquirePassword(); err != nil {
			return false, err
		}
		if bytes.Equal(CalcCachingSha2Password(c.salt, c.password), clientAuthData) {
			return false, nil
		}
		return false, errAccessDenied
	}
	// other type of credential provider, we use the cache
	cached, ok := c.serverConf.cacheShaPassword.Load(fmt.Sprintf("%s@%s", c.user, c.Conn.LocalAddr()))
	if ok {
		// Scramble validation
		if scrambleValidation(cached.([]byte), c.salt, clientAuthData) {
			return false, nil
		}
		return false, errAccessDenied
	}
	// cache miss, do full auth
	if err := c.writeAuthMoreDataFullAuth(); err != nil {
		return false, err
	}
	return true, nil
}