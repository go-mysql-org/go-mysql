package server

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

type AuthenticationProvider interface {
	Authenticate(c *Conn, authPluginName string, clientAuthData []byte) error
	Validate(authPluginName string) bool
}

type DefaultAuthenticationProvider struct{}

func (d *DefaultAuthenticationProvider) Authenticate(c *Conn, authPluginName string, clientAuthData []byte) error {
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

func (d *DefaultAuthenticationProvider) Validate(authPluginName string) bool {
	return isAuthMethodSupported(authPluginName)
}
