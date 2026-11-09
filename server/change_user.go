package server

import (
	"bytes"
	"encoding/binary"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ChangeUserHandler is implemented by a Handler that supports
// COM_CHANGE_USER. Without it the command reaches HandleOtherCommand.
//
// Before HandleChangeUser is called the server authenticates the new user
// through the connection's AuthenticationHandler (OnAuthFailure and
// OnAuthSuccess are called as for the handshake) and deallocates the
// connection's prepared statements (see Conn.ResetStmts). The handler then
// resets the session as for COM_RESET_CONNECTION and selects dbName, or no
// database when dbName is "".
//
// When authentication or HandleChangeUser fails, the error is sent to the
// client and the connection is closed, as MySQL does.
type ChangeUserHandler interface {
	HandleChangeUser(user string, dbName string) error
}

// changeUserRequest is a decoded COM_CHANGE_USER payload.
type changeUserRequest struct {
	user       string
	auth       []byte
	db         string
	charset    uint16 // collation ID; 0 when absent
	plugin     string
	attributes map[string]string // nil when absent
}

// handleChangeUser runs COM_CHANGE_USER. A failure closes the connection.
func (c *Conn) handleChangeUser(h ChangeUserHandler, data []byte) any {
	if err := c.changeUser(h, data); err != nil {
		_ = c.writeError(err)
		_ = c.Flush()
		c.Close()
		c.Conn = nil
		return noResponse{}
	}
	return nil
}

func (c *Conn) changeUser(h ChangeUserHandler, data []byte) error {
	req, err := c.parseChangeUser(data)
	if err != nil {
		return err
	}

	// Authenticate against the scramble the connection already has, as MySQL
	// does: the client computes its response from the handshake's (or the
	// last auth switch's) auth data.
	// The request's metadata is installed before authentication, as the
	// handshake installs its attributes, so the auth hooks see it.
	if req.charset != 0 {
		c.charset = uint8(req.charset)
		c.collationID = req.charset
	}
	if req.attributes != nil {
		c.attributes = req.attributes
	}
	c.user = req.user
	c.credential = Credential{}
	c.cachingSha2FullAuth = false
	c.authPluginName = req.plugin
	cont, err := c.handleAuthMatch(req.auth)
	if err == nil && cont {
		err = c.compareAuthData(c.authPluginName, req.auth)
	}
	if err != nil {
		err = c.accessDeniedError(err)
		c.authHandler.OnAuthFailure(c, err)
		return err
	}
	if err := c.authHandler.OnAuthSuccess(c); err != nil {
		return err
	}

	if err := c.ResetStmts(); err != nil {
		return err
	}
	return h.HandleChangeUser(req.user, req.db)
}

// parseChangeUser decodes a COM_CHANGE_USER payload (after the command byte).
// See https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_change_user.html
func (c *Conn) parseChangeUser(data []byte) (*changeUserRequest, error) {
	malformed := mysql.NewDefaultError(mysql.ER_MALFORMED_PACKET)
	req := &changeUserRequest{plugin: mysql.AUTH_NATIVE_PASSWORD}

	user, rest, ok := bytes.Cut(data, []byte{0x00})
	if !ok {
		return nil, malformed
	}
	req.user = string(user)

	// The handshake requires CLIENT_SECURE_CONNECTION, so the auth response
	// is always length-prefixed with one byte.
	if len(rest) < 1 || len(rest) < 1+int(rest[0]) {
		return nil, malformed
	}
	req.auth = append([]byte(nil), rest[1:1+int(rest[0])]...)
	rest = rest[1+int(rest[0]):]

	db, rest, ok := bytes.Cut(rest, []byte{0x00})
	if !ok {
		return nil, malformed
	}
	req.db = string(db)

	// Everything after the database is optional.
	if len(rest) == 0 {
		return req, nil
	}
	if len(rest) < 2 {
		return nil, malformed
	}
	req.charset = binary.LittleEndian.Uint16(rest)
	rest = rest[2:]

	if c.capability&mysql.CLIENT_PLUGIN_AUTH != 0 && len(rest) > 0 {
		plugin, after, ok := bytes.Cut(rest, []byte{0x00})
		if !ok {
			return nil, malformed
		}
		if len(plugin) > 0 {
			req.plugin = string(plugin)
		}
		rest = after
	}

	if c.capability&mysql.CLIENT_CONNECT_ATTRS != 0 && len(rest) > 0 {
		attrs, err := parseConnectAttributes(rest)
		if err != nil {
			return nil, malformed
		}
		req.attributes = attrs
	}
	return req, nil
}

// parseConnectAttributes decodes a length-encoded block of length-encoded
// key/value pairs.
func parseConnectAttributes(data []byte) (map[string]string, error) {
	total, isNull, n := mysql.LengthEncodedInt(data)
	if isNull || n == 0 || uint64(len(data)-n) < total {
		return nil, mysql.ErrMalformPacket
	}
	data = data[n : n+int(total)]
	attrs := make(map[string]string)
	for len(data) > 0 {
		key, _, n, err := mysql.LengthEncodedString(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]
		value, _, n, err := mysql.LengthEncodedString(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]
		attrs[string(key)] = string(value)
	}
	return attrs, nil
}
