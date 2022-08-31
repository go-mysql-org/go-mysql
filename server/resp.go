package server

import (
	"fmt"

	. "github.com/go-mysql-org/go-mysql/mysql"
)

func (c *Conn) writeOK(r *Result) error {
	if r == nil {
		r = &Result{}
	}

	r.Status |= c.status

	data := make([]byte, 4, 32)

	data = append(data, OK_HEADER)

	data = append(data, PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, PutLengthEncodedInt(r.InsertId)...)

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, byte(r.Warnings), byte(r.Warnings>>8))
	}

	return c.WritePacket(data)
}

func (c *Conn) writeError(e error) error {
	var m *MyError
	var ok bool
	if m, ok = e.(*MyError); !ok {
		m = NewError(ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.WritePacket(data)
}

func (c *Conn) writeEOF() error {
	data := make([]byte, 4, 9)

	data = append(data, EOF_HEADER)
	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(c.warnings), byte(c.warnings>>8))
		data = append(data, byte(c.status), byte(c.status>>8))
	}

	return c.WritePacket(data)
}

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
func (c *Conn) writeAuthSwitchRequest(newAuthPluginName string) error {
	data := make([]byte, 4)
	data = append(data, EOF_HEADER)
	data = append(data, []byte(newAuthPluginName)...)
	data = append(data, 0x00)
	// new auth data
	c.salt = RandomBuf(20)
	data = append(data, c.salt...)
	// the online doc states it's a string.EOF, however, the actual MySQL server add a \NUL to the end, without it, the
	// official MySQL client will fail.
	data = append(data, 0x00)
	return c.WritePacket(data)
}

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
func (c *Conn) readAuthSwitchRequestResponse() ([]byte, error) {
	data, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	if len(data) == 1 && data[0] == 0x00 {
		// \NUL
		return make([]byte, 0), nil
	}
	return data, nil
}

func (c *Conn) writeAuthMoreDataPubkey() error {
	data := make([]byte, 4)
	data = append(data, MORE_DATE_HEADER)
	data = append(data, c.serverConf.pubKey...)
	return c.WritePacket(data)
}

func (c *Conn) writeAuthMoreDataFullAuth() error {
	data := make([]byte, 4)
	data = append(data, MORE_DATE_HEADER)
	data = append(data, CACHE_SHA2_FULL_AUTH)
	return c.WritePacket(data)
}

func (c *Conn) writeAuthMoreDataFastAuth() error {
	data := make([]byte, 4)
	data = append(data, MORE_DATE_HEADER)
	data = append(data, CACHE_SHA2_FAST_AUTH)
	return c.WritePacket(data)
}

func (c *Conn) writeResultset(r *Resultset) error {
	// for a streaming resultset, that handled rowdata separately in a callback
	// of type SelectPerRowCallback, we can suffice by ending the stream with
	// an EOF
	// when streaming multiple queries, no EOF has to be sent, all results should've
	// been taken care of already in the user-defined callback
	if r.StreamingDone {
		switch r.Streaming {
		case StreamingMultiple:
			return nil
		case StreamingSelect:
			return c.writeEOF()
		}
	}

	columnLen := PutLengthEncodedInt(uint64(len(r.Fields)))

	data := make([]byte, 4, 1024)

	data = append(data, columnLen...)
	if err := c.WritePacket(data); err != nil {
		return err
	}

	if err := c.writeFieldList(r.Fields, data); err != nil {
		return err
	}

	// streaming select resultsets handle rowdata in a separate callback of type
	// SelectPerRowCallback so we're done here
	if r.Streaming == StreamingSelect {
		return nil
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}

	return nil
}

func (c *Conn) writeFieldList(fs []*Field, data []byte) error {
	if data == nil {
		data = make([]byte, 4, 1024)
	}

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}
	return nil
}

func (c *Conn) writeFieldValues(fv []FieldValue) error {
	data := make([]byte, 4, 1024)
	for _, v := range fv {
		if v.Value() == nil {
			// NULL value is encoded as 0xfb here
			data = append(data, []byte{0xfb}...)
		} else {
			tv, err := FormatTextValue(v.Value())
			if err != nil {
				return err
			}
			data = append(data, PutLengthEncodedString(tv)...)
		}
	}

	return c.WritePacket(data)
}

type noResponse struct{}
type eofResponse struct{}

func (c *Conn) WriteValue(value interface{}) error {
	switch v := value.(type) {
	case noResponse:
		return nil
	case eofResponse:
		return c.writeEOF()
	case error:
		return c.writeError(v)
	case nil:
		return c.writeOK(nil)
	case *Result:
		if v != nil && v.Resultset != nil {
			return c.writeResultset(v.Resultset)
		} else {
			return c.writeOK(v)
		}
	case []*Field:
		return c.writeFieldList(v, nil)
	case []FieldValue:
		return c.writeFieldValues(v)
	case *Stmt:
		return c.writePrepare(v)
	default:
		return fmt.Errorf("invalid response type %T", value)
	}
}
