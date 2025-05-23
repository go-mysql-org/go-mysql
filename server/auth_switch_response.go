package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/auth"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

func (c *Conn) handleAuthSwitchResponse() error {
	authData, err := c.readAuthSwitchRequestResponse()
	if err != nil {
		return err
	}

	return c.compareAuthData(c.authPluginName, authData)
}

func (c *Conn) handleCachingSha2PasswordFullAuth(authData []byte) error {
	if err := c.acquirePassword(); err != nil {
		return err
	}
	if tlsConn, ok := c.Conn.Conn.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			return errors.New("incomplete TSL handshake")
		}
		// connection is SSL/TLS, client should send plain password
		// deal with the trailing \NUL added for plain text password received
		if l := len(authData); l != 0 && authData[l-1] == 0x00 {
			authData = authData[:l-1]
		}
	} else {
		// client either request for the public key or send the encrypted password
		if len(authData) == 1 && authData[0] == 0x02 {
			// send the public key
			if err := c.writeAuthMoreDataPubkey(); err != nil {
				return err
			}
			// read the encrypted password
			var err error
			if authData, err = c.readAuthSwitchRequestResponse(); err != nil {
				return err
			}
		}
		// the encrypted password
		// decrypt
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.serverConf.tlsConfig.Certificates[0].PrivateKey).(*rsa.PrivateKey), authData, nil)
		if err != nil {
			return err
		}
		authData = mysql.Xor(dbytes, c.salt)
		if l := len(authData); l != 0 && authData[l-1] == 0x00 {
			authData = authData[:l-1]
		}
	}
	err := c.checkSha2CacheCredentials(authData, c.credential)
	if err != nil {
		return err
	}
	// write cache on successful auth - needs to be here as we have the decrypted password
	// and we need to store an unsalted hashed version of the plaintext password in the cache
	c.writeCachingSha2Cache(authData)
	return nil
}

func (c *Conn) checkSha2CacheCredentials(clientAuthData []byte, credential Credential) error {
	match, err := auth.CheckHashingPassword([]byte(credential.Password), string(clientAuthData), mysql.AUTH_CACHING_SHA2_PASSWORD)
	if match && err == nil {
		return nil
	}
	return errAccessDenied(credential)
}

func (c *Conn) writeCachingSha2Cache(authData []byte) {
	// write cache
	if authData == nil {
		return
	}

	if l := len(authData); l != 0 && authData[l-1] == 0x00 {
		authData = authData[:l-1]
	}
	// SHA256(PASSWORD)
	crypt := sha256.New()
	crypt.Write(authData)
	m1 := crypt.Sum(nil)
	// SHA256(SHA256(PASSWORD))
	crypt.Reset()
	crypt.Write(m1)
	m2 := crypt.Sum(nil)
	// caching_sha2_password will maintain an in-memory hash of `user`@`host` => SHA256(SHA256(PASSWORD))
	c.serverConf.cacheShaPassword.Store(fmt.Sprintf("%s@%s", c.user, c.LocalAddr()), m2)
}
