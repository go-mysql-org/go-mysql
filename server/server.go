package server

import (
	. "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/packet"
	"net"
	"sync/atomic"
)

type Conn struct {
	*packet.Conn

	capability uint32

	connectionID uint32

	status  uint16
	charset string

	user string
	db   string

	salt []byte
}

var baseConnID uint32 = 10000

func NewConn(conn net.Conn, password string) (*Conn, error) {
	c := new(Conn)

	c.Conn = packet.NewConn(conn)

	c.connectionID = atomic.AddUint32(&baseConnID, 1)

	//use default charset
	c.charset = DEFAULT_CHARSET

	if err := c.handshake(password); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Conn) handshake(password string) error {
	if err := c.writeInitialHandshake(); err != nil {
		return err
	}

	if err := c.readHandshakeResponse(password); err != nil {
		c.WriteError(err)

		return err
	}

	if err := c.WriteOK(nil); err != nil {
		return err
	}

	c.Conn.ResetSequence()

	return nil
}

func (c *Conn) Close() {
	if c.Conn != nil {
		c.Conn.Close()
		c.Conn = nil
	}
}

func (c *Conn) GetDB() string {
	return c.db
}
