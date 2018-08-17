package server

import (
	"bytes"
	"encoding/binary"

	. "github.com/siddontang/go-mysql/mysql"
	"github.com/juju/errors"
	"crypto/rsa"
	"crypto/rand"
	"crypto/sha1"
	"github.com/siddontang/go-log/log"
	"fmt"
	)

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
func (c *Conn) writeInitialHandshake() error {
	data := make([]byte, 4)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, c.serverConf.serverVersion...)
	data = append(data, 0x00)

	//connection id
	data = append(data, byte(c.connectionID), byte(c.connectionID>>8), byte(c.connectionID>>16), byte(c.connectionID>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter 0x00 byte, terminating the first part of a scramble
	data = append(data, 0x00)

	defaultFlag := c.serverConf.capability
	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(defaultFlag), byte(defaultFlag>>8))

	//charset
	data = append(data, c.serverConf.collationId)

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(defaultFlag>>16), byte(defaultFlag>>24))

	// server supports CLIENT_PLUGIN_AUTH and CLIENT_SECURE_CONNECTION
	data = append(data, byte(8+12))

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)
	// second part of the password cipher [mininum 13 bytes],
	// where len=MAX(13, length of auth-plugin-data - 8)
	// add \NUL to terminate the string
	data = append(data, 0x00)

	// auth plugin name
	data = append(data, c.serverConf.defaultAuthMethod...)

	// EOF if MySQL version (>= 5.5.7 and < 5.5.10) or (>= 5.6.0 and < 5.6.2)
	// \NUL otherwise, so we use \NUL
	data = append(data, 0)

	log.Debugf("writeInitialHandshake: %s", string(data))

	return c.WritePacket(data)
}

func (c *Conn) readHandshakeResponse(password string) error {
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}

	log.Debugf("readHandshakeResponse: handshake response: %s", string(data))

	pos := 0

	// check CLIENT_PROTOCOL_41
	if uint32(binary.LittleEndian.Uint16(data[:2])) & CLIENT_PROTOCOL_41 == 0 {
		return errors.New("CLIENT_PROTOCOL_41 compatible client is required")
	}

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	if c.capability & CLIENT_SECURE_CONNECTION  == 0 {
		return errors.New("CLIENT_SECURE_CONNECTION compatible client is required")
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
	if len(data) == (4+4+1+23) {
		if c.serverConf.capability & CLIENT_SSL == 0 {
			return errors.Errorf("The host '%s' does not support SSL connections", c.RemoteAddr().String())
		}
		// switched to SSL

		// go on handshake
		return c.readHandshakeResponse(password)
	}

	//user name
	user := string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
	pos += len(user) + 1

	if c.user != user {
		return NewDefaultError(ER_NO_SUCH_USER, user, c.RemoteAddr().String())
	}

	// length encoded data
	var auth []byte
	var authLen int
	if c.capability & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
		authData, isNULL, readBytes, err := LengthEncodedString(data[pos:])
		if err != nil {
			return err
		}
		if isNULL {
			// no auth length and no auth data, just \NUL, considered invalid auth data, and reject connection as MySQL does
			return NewDefaultError(ER_ACCESS_DENIED_ERROR, c.RemoteAddr().String(), c.user, "Yes")
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

	// if the client use 'sha256_password' auth method, and request for a public key
	// we send back a keyfile with Protocol::AuthMoreData
	// see: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
	if len(auth) == 1 && auth[0] == 0x01 {
		if c.serverConf.capability & CLIENT_SSL == 0 {
			return errors.New("server does not support SSL: CLIENT_SSL not enabled")
		}
		log.Debug("client requesting pub key")
		if err := c.writeAuthMoreData(); err != nil {
			return err
		}
		return c.handleAuthSwitchResponse(SHA256_PASSWORD, password)
	}

	pos += authLen

	if c.capability & CLIENT_CONNECT_WITH_DB != 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db := string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
		pos += len(db) + 1

		if err = c.h.UseDB(db); err != nil {
			return err
		}
	}
	if c.capability & CLIENT_PLUGIN_AUTH != 0 {
		c.authPluginName = string(data[pos : pos+bytes.IndexByte(data[pos:], 0x00)])
	} else {
		// The method used is Native Authentication if both CLIENT_PROTOCOL_41 and CLIENT_SECURE_CONNECTION are set,
		// but CLIENT_PLUGIN_AUTH is not set, so we fallback to 'mysql_native_password'
		c.authPluginName = MYSQL_NATIVE_PASSWORD
	}
	// since our proxy server does not lock the auth method as MySQL server does by setting user-level auth method
	// to override the global default auth method, we can try to meet client's auth method request in Handshake Response Packet
	// if our server also support the requested auth method. When it can not be met, like client wants 'mysql_old_password',
	// we issue a AuthSwitchRequest to the client with a server supported auth method.
	fmt.Println(c.authPluginName, c.serverConf.allowedAuthMethods)
	if !isAuthMethodAllowedByServer(c.authPluginName, c.serverConf.allowedAuthMethods) {
		log.Debugf("readHandshakeResponse: auth switch to: %s", c.serverConf.defaultAuthMethod)
		// this time, force the client to use the default auth method
		if err := c.writeAuthSwitchRequest(c.serverConf.defaultAuthMethod); err != nil {
			return err
		}
		// read the AuthSwitchResponse
		respData, err := c.readAuthSwitchRequestResponse()
		if err != nil {
			return err
		}
		auth = respData
		c.authPluginName = c.serverConf.defaultAuthMethod
	}

	// ignore connect attrs for now, the proxy does not support passing attrs to actual MySQL server

	// try to authenticate the client
	log.Debugf("readHandshakeResponse: auth method to compare: %s", c.authPluginName)
	ok, err := c.compareAuthData(c.authPluginName, auth, password)
	if err != nil {
		return err
	}
	if !ok {
		return NewDefaultError(ER_ACCESS_DENIED_ERROR, c.RemoteAddr().String(), c.user, "Yes")
	}

	return nil
}

func (c *Conn) handleAuthSwitchResponse(authPluginName, password string) error {
	authData, err := c.ReadPacket()
	if err != nil {
		return err
	}

	log.Debugf("handleAuthSwitchResponse: auth switch response: %s", string(authData))

	ok, err := c.compareAuthSwitchData(authPluginName, authData, password)
	if err != nil {
		return err
	}
	if !ok {
		return NewDefaultError(ER_ACCESS_DENIED_ERROR, c.RemoteAddr().String(), c.user, "Yes")
	}
	return nil
}

func (c *Conn) compareAuthData(authPluginName string, clientAuthData []byte, password string) (bool, error) {
	switch authPluginName {
	case MYSQL_NATIVE_PASSWORD:
		return bytes.Equal(CalcPassword(c.salt, []byte(password)), clientAuthData), nil

	case CACHING_SHA2_PASSWORD:
		// Empty passwords are not hashed, but sent as empty string
		if len(clientAuthData) == 0 && password == "" {
			return  true, nil
		}
		// the caching in MySQL, see: https://dev.mysql.com/worklog/task/?id=9591
		// since we have already kept the password in memory and calculate the scramble is not that high of cost, we eliminate
		// the caching part. So our server will never ask the client to do a full authentication via RSA key exchange and it appears
		// like the auth will always hit the cache.
		return bytes.Equal(CalcCachingSha2Password(c.salt, password), clientAuthData), nil

	case SHA256_PASSWORD:
		// Empty passwords are not hashed, but sent as empty string
		if len(clientAuthData) == 0 && password == "" {
			return  true, nil
		}
		// only if the connection is SSL/TLS
		return bytes.Equal(clientAuthData, []byte(password)), nil

	default:
		return false, errors.Errorf("unknown authentication plugin name")
	}
}

func (c *Conn) compareAuthSwitchData(authPluginName string, clientAuthData []byte, password string) (bool, error) {
	switch authPluginName {
	case MYSQL_NATIVE_PASSWORD:
		return bytes.Equal(CalcPassword(c.salt, []byte(password)), clientAuthData), nil

	case CACHING_SHA2_PASSWORD:
		// this should never happen since we force it to be 'fast auth'
		return false, errors.New("this server only supports 'caching_sha2_password' fast auth")

	case SHA256_PASSWORD:
		// Empty passwords are not hashed, but sent as empty string
		if len(clientAuthData) == 0 && password == "" {
			return  true, nil
		}
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

	default:
		return false, errors.Errorf("unknown authentication plugin name")
	}
}