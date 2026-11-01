package packet

import (
	"bytes"
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

type bufferHintConn struct {
	net.Conn
	reader *bytes.Reader
	reads  int
}

func (c *bufferHintConn) Read(p []byte) (int, error) { c.reads++; return c.reader.Read(p) }

func TestBufferedPacketSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		tail []byte
		want int
	}{
		{"complete", []byte{2, 0, 0, 1, 'a', 'b'}, 2},
		{"empty", []byte{0, 0, 0, 1}, 0},
		{"partial-header", []byte{2, 0}, -1},
		{"partial-body", []byte{2, 0, 0, 1, 'a'}, -1},
		{"continuation", []byte{255, 255, 255, 1}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := &bufferHintConn{reader: bytes.NewReader(append([]byte{1, 0, 0, 0, 'x'}, tc.tail...))}
			c := NewBufferedConn(raw, 4096)
			require.Equal(t, -1, c.BufferedPacketSize())
			require.Zero(t, raw.reads)
			p, err := c.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, []byte{'x'}, p)
			require.Equal(t, tc.want, c.BufferedPacketSize())
			require.Equal(t, 1, raw.reads)
			c.Compression = mysql.MYSQL_COMPRESS_ZLIB
			require.Equal(t, -1, c.BufferedPacketSize())
			c.Compression = mysql.MYSQL_COMPRESS_NONE
			require.Equal(t, tc.want, c.BufferedPacketSize())
			if tc.want >= 0 {
				p, err = c.ReadPacket()
				require.NoError(t, err)
				require.Equal(t, string(tc.tail[4:]), string(p))
			}
		})
	}
	require.Equal(t, -1, NewTLSConn(&bufferHintConn{reader: bytes.NewReader(nil)}).BufferedPacketSize())
}
