package server

import (
	"context"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func (c *Conn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = mysql.NewResultReserveResultset(0)
	}

	r.Status |= c.status

	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, byte(r.Warnings), byte(r.Warnings>>8))
	}

	return c.WritePacket(data)
}

func (c *Conn) writeError(e error) error {
	var m *mysql.MyError
	var ok bool
	if m, ok = e.(*mysql.MyError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.WritePacket(data)
}

func (c *Conn) writeEOF() error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(c.warnings), byte(c.warnings>>8))
		data = append(data, byte(c.status), byte(c.status>>8))
	}

	return c.WritePacket(data)
}

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
func (c *Conn) writeAuthSwitchRequest(newAuthPluginName string) error {
	c.authPluginName = newAuthPluginName
	data := make([]byte, 4)
	data = append(data, mysql.EOF_HEADER)
	data = append(data, []byte(newAuthPluginName)...)
	data = append(data, 0x00)
	// new auth data
	c.salt = mysql.RandomBuf(20)
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
	data = append(data, mysql.MORE_DATE_HEADER)
	data = append(data, c.serverConf.pubKey...)
	return c.WritePacket(data)
}

func (c *Conn) writeAuthMoreDataFullAuth() error {
	data := make([]byte, 4)
	data = append(data, mysql.MORE_DATE_HEADER)
	data = append(data, mysql.CACHE_SHA2_FULL_AUTH)
	return c.WritePacket(data)
}

func (c *Conn) writeAuthMoreDataFastAuth() error {
	data := make([]byte, 4)
	data = append(data, mysql.MORE_DATE_HEADER)
	data = append(data, mysql.CACHE_SHA2_FAST_AUTH)
	return c.WritePacket(data)
}

func (c *Conn) writeResultset(r *mysql.Resultset) error {
	// for a streaming resultset, that handled rowdata separately in a callback
	// of type SelectPerRowCallback, we can suffice by ending the stream with
	// an EOF
	// when streaming multiple queries, no EOF has to be sent, all results should've
	// been taken care of already in the user-defined callback
	if r.StreamingDone {
		switch r.Streaming {
		case mysql.StreamingMultiple:
			return nil
		case mysql.StreamingSelect:
			return c.writeEOF()
		}
	}

	columnLen := mysql.PutLengthEncodedInt(uint64(len(r.Fields)))

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
	if r.Streaming == mysql.StreamingSelect {
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

func (c *Conn) writeFieldList(fs []*mysql.Field, data []byte) error {
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

func (c *Conn) writeFieldValues(fv []mysql.FieldValue) error {
	data := make([]byte, 4, 1024)
	for _, v := range fv {
		if v.Value() == nil {
			// NULL value is encoded as 0xfb here
			data = append(data, []byte{0xfb}...)
		} else {
			tv, err := mysql.FormatTextValue(v.Value())
			if err != nil {
				return err
			}
			data = append(data, mysql.PutLengthEncodedString(tv)...)
		}
	}

	return c.WritePacket(data)
}

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication.html
func (c *Conn) writeBinlogEvents(s *replication.BinlogStreamer) error {
	for {
		ev, err := s.GetEvent(context.Background())
		if err != nil {
			return err
		}
		data := make([]byte, 4, 4+len(ev.RawData))
		data = append(data, mysql.OK_HEADER)

		data = append(data, ev.RawData...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}
}

type (
	noResponse  struct{}
	eofResponse struct{}
)

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
	case *mysql.Result:
		if v != nil && v.HasResultset() {
			return c.writeResultset(v.Resultset)
		} else {
			return c.writeOK(v)
		}
	case []*mysql.Field:
		return c.writeFieldList(v, nil)
	case []mysql.FieldValue:
		return c.writeFieldValues(v)
	case *replication.BinlogStreamer:
		return c.writeBinlogEvents(v)
	case *Stmt:
		return c.writePrepare(v)
	default:
		return fmt.Errorf("invalid response type %T", value)
	}
}
