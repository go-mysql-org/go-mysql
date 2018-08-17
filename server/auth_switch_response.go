package server

import (
	"github.com/siddontang/go-log/log"
	. "github.com/siddontang/go-mysql/mysql"
	"bytes"
	"github.com/juju/errors"
	"crypto/tls"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/rand"
)

func (c *Conn) handleAuthSwitchResponse(moreDataSent bool) error {
	authData, err := c.readAuthSwitchRequestResponse()
	if err != nil {
		return err
	}

	log.Debugf("handleAuthSwitchResponse: auth switch response: %s", string(authData))
	accessDeniedErr := NewDefaultError(ER_ACCESS_DENIED_ERROR, c.LocalAddr().String(), c.user, "Yes")
	switch c.authPluginName {
	case MYSQL_NATIVE_PASSWORD:
		if err := c.acquirePassword(); err != nil {
			return err
		}
		if !bytes.Equal(CalcPassword(c.salt, []byte(c.password)), authData) {
			return accessDeniedErr
		}
		return nil

	case CACHING_SHA2_PASSWORD:
		if moreDataSent {
			if err := c.acquirePassword(); err != nil {
				return err
			}
			if tlsConn, ok := c.Conn.Conn.(*tls.Conn); ok {
				if !tlsConn.ConnectionState().HandshakeComplete {
					return errors.New("incomplete TSL handshake")
				}
				// connection is SSL/TLS, client should send plain password
				if bytes.Equal(authData, []byte(c.password)) {
					return nil
				}
				return accessDeniedErr
			} else {
				// client either request for the public key or send the encrypted password
				data, err := c.ReadPacket()
				if err != nil {
					return err
				}
				if len(data) == 1 && data[0] == 0x02 {
					// send the public key
					if err := c.writeAuthMoreDataPubkey(); err != nil {
						return err
					}
					// read the encrypted password
					data, err = c.readAuthSwitchRequestResponse()
				}
				// the encrypted password
				// decrypt
				log.Debug("handleAuthSwitchResponse: decrypt a SHA2_PASSWORD")
				dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.serverConf.tlsConfig.Certificates[0].PrivateKey).(*rsa.PrivateKey), data, nil)
				if err != nil {
					return err
				}
				plain := make([]byte, len(c.password)+1)
				copy(plain, c.password)
				for i := range plain {
					j := i % len(c.salt)
					plain[i] ^= c.salt[j]
				}
				if bytes.Equal(plain, dbytes) {
					return nil
				}
				return accessDeniedErr
			}
		}

		// Switched auth method but no MoreData packet send yet
		if switched, err := c.compareCacheSha2PasswordAuthData(authData); err != nil {
			return err
		} else {
			return c.handleAuthSwitchResponse(switched)
		}

	case SHA256_PASSWORD:
		if err := c.acquirePassword(); err != nil {
			return err
		}
		ok, err := c.compareSha256PasswordAuthData(authData, c.password)
		if err != nil {
			return err
		}
		if !ok {
			return accessDeniedErr
		}
		return nil

	default:
		return errors.Errorf("unknown authentication plugin name")
	}
}
