package mysql

import (
	"fmt"
	"io"
	"net"
)

type PacketIO struct {
	Conn     net.Conn
	Sequence uint8
}

func (c *PacketIO) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *PacketIO) LocalAddr() net.Addr {
	return c.Conn.LocalAddr()
}

func (c *PacketIO) ReadPacket() ([]byte, error) {
	header := make([]byte, 4)

	if _, err := io.ReadFull(c.Conn, header); err != nil {
		return nil, ErrBadConn
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		err := fmt.Errorf("invalid payload length %d", length)
		return nil, err
	}

	sequence := uint8(header[3])

	if sequence != c.Sequence {
		err := fmt.Errorf("invalid sequence %d != %d", sequence, c.Sequence)
		return nil, err
	}

	c.Sequence++

	data := make([]byte, length)
	if _, err := io.ReadFull(c.Conn, data); err != nil {
		return nil, ErrBadConn
	} else {
		if length < MaxPayloadLen {
			return data, nil
		}

		var buf []byte
		buf, err = c.ReadPacket()
		if err != nil {
			return nil, ErrBadConn
		} else {
			return append(data, buf...), nil
		}
	}
}

//data already have header
func (c *PacketIO) WritePacket(data []byte) error {
	length := len(data) - 4

	for length >= MaxPayloadLen {

		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = c.Sequence

		if n, err := c.Conn.Write(data[:4+MaxPayloadLen]); err != nil {
			return ErrBadConn
		} else if n != (4 + MaxPayloadLen) {
			return ErrBadConn
		} else {
			c.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = c.Sequence

	if n, err := c.Conn.Write(data); err != nil {
		return ErrBadConn
	} else if n != len(data) {
		return ErrBadConn
	} else {
		c.Sequence++
		return nil
	}
}
