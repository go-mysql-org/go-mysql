package client

import (
	"encoding/binary"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestHandleOKPacketPreservesSuffix(t *testing.T) {
	c := &Conn{}
	c.capability = mysql.CLIENT_PROTOCOL_41 | mysql.CLIENT_SESSION_TRACK

	data := []byte{mysql.OK_HEADER}
	data = append(data, mysql.PutLengthEncodedInt(0)...)
	data = append(data, mysql.PutLengthEncodedInt(0)...)
	data = binary.LittleEndian.AppendUint16(data, 0) // status
	data = binary.LittleEndian.AppendUint16(data, 0) // warnings
	suffix := []byte{0x01, 0xfe, 0xaa, 0xbb}
	data = append(data, suffix...)

	r, err := c.handleOKPacket(data)
	require.NoError(t, err)
	require.Equal(t, suffix, r.OKPayloadSuffix)
}
