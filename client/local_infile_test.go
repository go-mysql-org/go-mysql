package client

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/stretchr/testify/require"
)

func newLocalInfileTestConn(server net.Conn) *Conn {
	return &Conn{
		Conn:       packet.NewConn(server),
		capability: mysql.CLIENT_PROTOCOL_41,
	}
}

func writeServerPacket(pc *packet.Conn, payload []byte) error {
	data := make([]byte, 4+len(payload))
	copy(data[4:], payload)
	return pc.WritePacket(data)
}

func writeServerOK(pc *packet.Conn) error {
	payload := []byte{mysql.OK_HEADER, 0, 0, 0x02, 0x00, 0, 0}
	return writeServerPacket(pc, payload)
}

func writeServerERR(pc *packet.Conn) error {
	payload := []byte{
		mysql.ERR_HEADER,
		0x01, 0x00,
		'#', 'H', 'Y', '0', '0', '0',
	}
	payload = append(payload, []byte("test error")...)
	return writeServerPacket(pc, payload)
}

func writeServerLocalInfile(pc *packet.Conn, filename string) error {
	payload := append([]byte{mysql.LocalInFile_HEADER}, []byte(filename)...)
	return writeServerPacket(pc, payload)
}

func readServerCOMQuery(t *testing.T, pc *packet.Conn) {
	t.Helper()
	pkt, err := pc.ReadPacket()
	require.NoError(t, err)
	require.NotEmpty(t, pkt)
	require.Equal(t, mysql.COM_QUERY, pkt[0])
}

type failOnWriteConn struct {
	net.Conn
	writes int
	failOn int
}

func (c *failOnWriteConn) Write(b []byte) (int, error) {
	c.writes++
	if c.writes == c.failOn {
		return 0, errors.New("write failed")
	}
	return c.Conn.Write(b)
}

type twoChunkReader struct {
	chunks [][]byte
	i      int
}

func (r *twoChunkReader) Read(p []byte) (int, error) {
	if r.i >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.i])
	r.i++
	if r.i >= len(r.chunks) {
		return n, io.EOF
	}
	return n, nil
}

func (r *twoChunkReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart || offset != 0 {
		return 0, errors.New("unsupported seek")
	}
	r.i = 0
	return 0, nil
}

func TestExecQueryRelayLocalInfile(t *testing.T) {
	t.Run("DirectOK", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		serverPC := packet.NewConn(serverConn)
		go func() {
			readServerCOMQuery(t, serverPC)
			require.NoError(t, writeServerOK(serverPC))
		}()

		c := newLocalInfileTestConn(clientConn)
		relayCalled := false
		result, err := c.ExecQueryRelayLocalInfile("LOAD DATA LOCAL INFILE 'f.csv'", func([]byte) (io.Reader, error) {
			relayCalled = true
			return nil, nil
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.False(t, relayCalled)
	})

	t.Run("DirectErr", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		serverPC := packet.NewConn(serverConn)
		go func() {
			readServerCOMQuery(t, serverPC)
			require.NoError(t, writeServerERR(serverPC))
		}()

		c := newLocalInfileTestConn(clientConn)
		relayCalled := false
		result, err := c.ExecQueryRelayLocalInfile("LOAD DATA LOCAL INFILE 'f.csv'", func([]byte) (io.Reader, error) {
			relayCalled = true
			return nil, nil
		})
		require.Error(t, err)
		require.Nil(t, result)
		require.False(t, relayCalled)
	})

	t.Run("RelaySuccess", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		serverPC := packet.NewConn(serverConn)
		go func() {
			readServerCOMQuery(t, serverPC)
			require.NoError(t, writeServerLocalInfile(serverPC, "test.csv"))

			pkt, err := serverPC.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, []byte("row1\n"), pkt)

			pkt, err = serverPC.ReadPacket()
			require.NoError(t, err)
			require.Empty(t, pkt)

			require.NoError(t, writeServerOK(serverPC))
		}()

		c := newLocalInfileTestConn(clientConn)
		var filename []byte
		result, err := c.ExecQueryRelayLocalInfile("LOAD DATA LOCAL INFILE 'test.csv'", func(fn []byte) (io.Reader, error) {
			filename = append([]byte(nil), fn...)
			return bytes.NewReader([]byte("row1\n")), nil
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "test.csv", string(filename))
	})

	t.Run("RelayError", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		relayErr := errors.New("relay failed")

		serverPC := packet.NewConn(serverConn)
		go func() {
			readServerCOMQuery(t, serverPC)
			require.NoError(t, writeServerLocalInfile(serverPC, "test.csv"))

			pkt, err := serverPC.ReadPacket()
			require.NoError(t, err)
			require.Empty(t, pkt)

			require.NoError(t, writeServerOK(serverPC))
		}()

		c := newLocalInfileTestConn(clientConn)
		_, err := c.ExecQueryRelayLocalInfile("LOAD DATA LOCAL INFILE 'test.csv'", func([]byte) (io.Reader, error) {
			return nil, relayErr
		})
		require.ErrorIs(t, err, relayErr)
	})

	t.Run("RelayReadError", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()

		readErr := errors.New("read failed mid-transfer")

		serverPC := packet.NewConn(serverConn)
		go func() {
			readServerCOMQuery(t, serverPC)
			require.NoError(t, writeServerLocalInfile(serverPC, "test.csv"))

			pkt, err := serverPC.ReadPacket()
			require.NoError(t, err)
			require.Empty(t, pkt, "server must receive only empty terminator, no partial file data")

			require.NoError(t, writeServerOK(serverPC))
		}()

		c := newLocalInfileTestConn(clientConn)
		_, err := c.ExecQueryRelayLocalInfile("LOAD DATA LOCAL INFILE 'test.csv'", func([]byte) (io.Reader, error) {
			return &failingLocalInfileReader{data: []byte("partial"), err: readErr}, nil
		})
		require.ErrorIs(t, err, readErr)
	})

	t.Run("StreamWriteError", func(t *testing.T) {
		serverConn, rawClientConn := net.Pipe()
		defer serverConn.Close()
		defer rawClientConn.Close()

		// COM_QUERY (1), first chunk (2), second chunk fails (3), defensive terminator (4).
		clientConn := &failOnWriteConn{Conn: rawClientConn, failOn: 3}

		serverPC := packet.NewConn(serverConn)
		go func() {
			readServerCOMQuery(t, serverPC)
			require.NoError(t, writeServerLocalInfile(serverPC, "test.csv"))

			pkt, err := serverPC.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, []byte("part1"), pkt)

			pkt, err = serverPC.ReadPacket()
			require.NoError(t, err)
			require.Empty(t, pkt, "defensive terminator must be sent after write failure")

			require.NoError(t, writeServerOK(serverPC))
		}()

		c := newLocalInfileTestConn(clientConn)
		result, err := c.ExecQueryRelayLocalInfile("LOAD DATA LOCAL INFILE 'test.csv'", func([]byte) (io.Reader, error) {
			return &twoChunkReader{chunks: [][]byte{[]byte("part1"), []byte("part2")}}, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "write failed")
		require.Nil(t, result)
	})
}

type failingLocalInfileReader struct {
	data []byte
	pos  int
	err  error
}

func (r *failingLocalInfileReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, r.err
	}
	return n, nil
}
