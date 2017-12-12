package packet

import (
	"bufio"
	"io"
	"net"

	"github.com/juju/errors"
	. "github.com/siddontang/go-mysql/mysql"
)

/*
	Conn is the base class to handle MySQL protocol.
*/
type Conn struct {
	net.Conn
	br *bufio.Reader

	Sequence uint8
}

func NewConn(conn net.Conn) *Conn {
	c := new(Conn)

	c.br = bufio.NewReaderSize(conn, 4096)
	c.Conn = conn

	return c
}

func (c *Conn) ReadPacket() ([]byte, error) {
	var prevData []byte
	for {
		// read packet header
		header := []byte{0, 0, 0, 0}
		if _, err := io.ReadFull(c.br, header); err != nil {
			return nil, ErrBadConn
		}

		// packet length [24 bit]
		length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

		// check packet sync [8 bit]
		sequence := uint8(header[3])
		if sequence != c.Sequence {
			return nil, errors.Errorf("invalid sequence %d != %d", sequence, c.Sequence)
		}
		c.Sequence++

		// packets with length 0 terminate a previous packet which is a
		// multiple of (2^24)−1 bytes long
		if length == 0 {
			// there was no previous packet
			if prevData == nil {
				return nil, errors.Errorf("invalid payload length %d", length)
			}
			return prevData, nil
		}

		// read packet body [length bytes]
		data := make([]byte, length)
		if _, err := io.ReadFull(c.br, data); err != nil {
			return nil, ErrBadConn
		}

		// return data if this was the last packet
		if length < MaxPayloadLen {
			// zero allocations for non-split packets
			if prevData == nil {
				return data, nil
			}

			return append(prevData, data...), nil
		}
		prevData = append(prevData, data...)
	}
}

// data already has 4 bytes header
// will modify data inplace
func (c *Conn) WritePacket(data []byte) error {
	length := len(data) - 4

	for {
		var size int
		if length >= MaxPayloadLen {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = MaxPayloadLen
		} else {
			data[0] = byte(length)
			data[1] = byte(length >> 8)
			data[2] = byte(length >> 16)
			size = length
		}
		data[3] = c.Sequence

		if n, err := c.Write(data[:4+size]); err != nil {
			return ErrBadConn
		} else if n != (4 + size) {
			return ErrBadConn
		} else {
			c.Sequence++
			if size != MaxPayloadLen {
				return nil
			}
			length -= size
			data = data[size:]
			continue
		}
	}
}

func (c *Conn) ResetSequence() {
	c.Sequence = 0
}

func (c *Conn) Close() error {
	c.Sequence = 0
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}
