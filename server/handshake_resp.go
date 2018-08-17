package server

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	. "github.com/siddontang/go-mysql/mysql"
)

func (c *Conn) readHandshakeResponse() error {
	data, pos, err := c.readFirstPart()
	if err != nil {
		return err
	}
	if pos, err = c.readUserName(data, pos); err != nil {
		return err
	}
	authData, authLen, pos, err := c.readAuthData(data, pos)
	if err != nil {
		return err
	}
	cont, err := c.handlePublicKeyRetrieval(authData, pos)
	if err != nil {
		return err
	}
	if !cont {
		return nil
	}

	pos += authLen

	if pos, err = c.readDb(data, pos); err != nil {
		return err
	}

	pos = c.readPluginName(data, pos)

	if cont, err = c.handleAuthMatch(authData, pos, c.password); err != nil {
		return err
	}
	if !cont {
		return nil
	}

	// ignore connect attrs for now, the proxy does not support passing attrs to actual MySQL server

	// try to authenticate the client
	log.Debugf("readHandshakeResponse: auth method to compare: %s", c.authPluginName)
	if err = c.compareAuthData(c.authPluginName, authData); err != nil {
		return err
	}
	return c.handleAuthSwitchResponse()
}

func (c *Conn) readFirstPart() ([]byte, int, error) {
	data, err := c.ReadPacket()
	if err != nil {
		return nil, 0, err
	}

	log.Debugf("readHandshakeResponse: handshake response: %s", string(data))

	pos := 0

	// check CLIENT_PROTOCOL_41
	if uint32(binary.LittleEndian.Uint16(data[:2]))&CLIENT_PROTOCOL_41 == 0 {
		return nil, 0, errors.New("CLIENT_PROTOCOL_41 compatible client is required")
	}

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	if c.capability&CLIENT_SECURE_CONNECTION == 0 {
		return nil, 0, errors.New("CLIENT_SECURE_CONNECTION compatible client is required")
	}
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	// is this a SSLRequest packet?
	if len(data) == (4 + 4 + 1 + 23) {
		if c.serverConf.capability&CLIENT_SSL == 0 {
			return nil, 0, errors.Errorf("The host '%s' does not support SSL connections", c.RemoteAddr().String())
		}
		// switch to TLS
		tlsConn := tls.Client(c.Conn.Conn, c.serverConf.tlsConfig)
		c.Conn.Conn = tlsConn

		// handshake again
		return c.readFirstPart()
	}
	return data, pos, nil
}

func (c *Conn) readUserName(data []byte, pos int) (int, error) {
	//user name
	user := string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
	pos += len(user) + 1
	c.user = user
	return pos, nil
}

func (c *Conn) readDb(data []byte, pos int) (int, error) {
	if c.capability&CLIENT_CONNECT_WITH_DB != 0 {
		if len(data[pos:]) == 0 {
			return pos, nil
		}

		db := string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
		pos += len(db) + 1

		if err := c.h.UseDB(db); err != nil {
			return 0, err
		}
	}
	return pos, nil
}

func (c *Conn) readPluginName(data []byte, pos int) int {
	if c.capability&CLIENT_PLUGIN_AUTH != 0 {
		c.authPluginName = string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
		pos += len(c.authPluginName)
	} else {
		// The method used is Native Authentication if both CLIENT_PROTOCOL_41 and CLIENT_SECURE_CONNECTION are set,
		// but CLIENT_PLUGIN_AUTH is not set, so we fallback to 'mysql_native_password'
		c.authPluginName = MYSQL_NATIVE_PASSWORD
	}
	return pos
}

func (c *Conn) readAuthData(data []byte, pos int) ([]byte, int, int, error) {
	// length encoded data
	var auth []byte
	var authLen int
	if c.capability&CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
		authData, isNULL, readBytes, err := LengthEncodedString(data[pos:])
		if err != nil {
			return nil, 0, 0, err
		}
		if isNULL {
			// no auth length and no auth data, just \NUL, considered invalid auth data, and reject connection as MySQL does
			return nil, 0, 0, NewDefaultError(ER_ACCESS_DENIED_ERROR, c.LocalAddr().String(), c.user, "Yes")
		}
		auth = authData
		authLen = readBytes
	} else {
		//auth length and auth
		authLen = int(data[pos])
		pos++
		auth = data[pos : pos+authLen]
		if authLen == 0 {
			// skip the next \NUL in case the password is empty
			pos++
		}
	}
	return auth, authLen, pos, nil
}

// Public Key Retrieval
// See: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
func (c *Conn) handlePublicKeyRetrieval(authData []byte, pos int) (bool, error) {
	// if the client use 'sha256_password' auth method, and request for a public key
	// we send back a keyfile with Protocol::AuthMoreData
	if len(authData) == 1 && authData[0] == 0x01 {
		if c.serverConf.capability&CLIENT_SSL == 0 {
			return false, errors.New("server does not support SSL: CLIENT_SSL not enabled")
		}
		log.Debug("client requesting pub key")
		if err := c.writeAuthMoreDataPubkey(); err != nil {
			return false, err
		}
		c.authPluginName = SHA256_PASSWORD
		return false, c.handleAuthSwitchResponse()
	}
	return true, nil
}

func (c *Conn) handleAuthMatch(authData []byte, pos int, password string) (bool, error) {
	// since our proxy server does not lock the auth method as MySQL server does by setting user-level auth method
	// to override the global default auth method, we can try to meet client's auth method request in Handshake Response Packet
	// if our server also support the requested auth method. When it can not be met, like client wants 'mysql_old_password',
	// we issue a AuthSwitchRequest to the client with a server supported auth method.
	fmt.Println(c.authPluginName, c.serverConf.allowedAuthMethods)
	if !isAuthMethodAllowedByServer(c.authPluginName, c.serverConf.allowedAuthMethods) {
		log.Debugf("readHandshakeResponse: auth switch to: %s", c.serverConf.defaultAuthMethod)
		// this time, force the client to use the default auth method
		if err := c.writeAuthSwitchRequest(c.serverConf.defaultAuthMethod); err != nil {
			return false, err
		}
		c.authPluginName = c.serverConf.defaultAuthMethod
		// handle AuthSwitchResponse
		return false, c.handleAuthSwitchResponse()
	}
	return true, nil
}
