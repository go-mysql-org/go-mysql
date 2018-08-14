package client

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"

	"github.com/juju/errors"
	. "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/packet"
	"fmt"
)

// See: http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
func (c *Conn) readInitialHandshake() error {
	data, err := c.ReadPacket()
	if err != nil {
		return errors.Trace(err)
	}

	if data[0] == ERR_HEADER {
		return errors.New("read initial handshake error")
	}

	if data[0] < MinProtocolVersion {
		return errors.Errorf("invalid protocol version %d, must >= 10", data[0])
	}

	//skip mysql version
	//mysql version end with 0x00
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1

	//connection id length is 4
	c.connectionID = uint32(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4

	c.salt = []byte{}
	c.salt = append(c.salt, data[pos:pos+8]...)

	//skip filter
	pos += 8 + 1

	//capability lower 2 bytes
	c.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))
	// check protocol
	if c.capability&CLIENT_PROTOCOL_41 == 0 {
		return errors.New("the MySQL server can not support protocol 41 and above required by the client")
	}
	if c.capability&CLIENT_SSL == 0 && c.TLSConfig != nil {
		return errors.New("the MySQL Server does not support TLS required by the client")
	}
	pos += 2

	if len(data) > pos {
		//skip server charset
		//c.charset = data[pos]
		pos += 1

		c.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2
		//capability flags (upper 2 bytes)
		c.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | c.capability
		pos += 2

		//skip auth data len or [00]
		//skip reserved (all [00])
		pos += 10 + 1

		// The documentation is ambiguous about the length.
		// The official Python library uses the fixed length 12
		// mysql-proxy also use 12
		// which is not documented but seems to work.
		if c.capability&CLIENT_SECURE_CONNECTION != 0 {
			c.salt = append(c.salt, data[pos:pos+12]...)
			pos += 13
		}
		// auth plugin
		if c.capability&CLIENT_PLUGIN_AUTH != 0 {
			if end := bytes.IndexByte(data[pos:], 0x00); end != -1 {
				c.authPluginName = string(data[pos : pos+end])
			} else {
				c.authPluginName = string(data[pos:])
			}
		}
	}

	return nil
}

// See: http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (c *Conn) writeAuthHandshake() error {
	if c.authPluginName != "mysql_native_password" && c.authPluginName != "caching_sha2_password" && c.authPluginName != "sha256_password" {
		return fmt.Errorf("unknow auth plugin name '%s'", c.authPluginName)
	}

	// Adjust client capability flags based on server support
	capability := CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_PLUGIN_AUTH | (c.capability&CLIENT_LONG_FLAG)

	// To enable TLS / SSL
	if c.TLSConfig != nil {
		capability |= CLIENT_SSL
	}

	// password hashing
	var auth []byte
	switch c.authPluginName {
	case "mysql_native_password":
		auth = CalcPassword(c.salt, []byte(c.password))
	case "caching_sha2_password":
		auth = CaclCachingSha2Password(c.salt, []byte(c.password))
	case "sha256_password":
		if len(c.password) == 0 {
			auth = nil
		}
		if c.TLSConfig != nil || c.proto == "unix" {
			// write cleartext auth packet
			auth = []byte(c.password)
		} else {
			// request public key from server
			// see: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
			auth = []byte{1}
		}
	}

	//packet length
	//capability 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	//username
	//auth
	//mysql_native_password + null-terminated
	length := 4 + 4 + 1 + 23 + len(c.user) + 1 + 1 + len(auth) + 21 + 1
	// db name
	if len(c.db) > 0 {
		capability |= CLIENT_CONNECT_WITH_DB
		length += len(c.db) + 1
	}

	data := make([]byte, length+4)

	//capability [32 bit]
	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	//MaxPacketSize [32 bit] (none)
	data[8] = 0x00
	data[9] = 0x00
	data[10] = 0x00
	data[11] = 0x00

	//Charset [1 byte]
	//use default collation id 33 here, is utf-8
	data[12] = byte(DEFAULT_COLLATION_ID)

	// SSL Connection Request Packet
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if c.TLSConfig != nil {
		// Send TLS / SSL request packet
		if err := c.WritePacket(data[:(4+4+1+23)+4]); err != nil {
			return err
		}

		// Switch to TLS
		tlsConn := tls.Client(c.Conn.Conn, c.TLSConfig)
		if err := tlsConn.Handshake(); err != nil {
			return err
		}

		currentSequence := c.Sequence
		c.Conn = packet.NewConn(tlsConn)
		c.Sequence = currentSequence
	}

	//Filler [23 bytes] (all 0x00)
	pos := 13
	for ; pos < 13+23; pos++ {
		data[pos] = 0
	}

	//User [null terminated string]
	if len(c.user) > 0 {
		pos += copy(data[pos:], c.user)
	}
	data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	if auth == nil {
		data[pos] = 0x00
		pos++
	}else {
		data[pos] = byte(len(auth))
		pos += 1 + copy(data[pos+1:], auth)
	}
	// db [null terminated string]
	if len(c.db) > 0 {
		pos += copy(data[pos:], c.db)
		data[pos] = 0x00
		pos++
	}

	// Assume native client during response
	pos += copy(data[pos:], c.authPluginName)
	data[pos] = 0x00

	fmt.Println(string(data))

	return c.WritePacket(data)
}
