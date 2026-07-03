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

func TestExecQueryRelayLocalInfile_DirectOK(t *testing.T) {
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
}

func TestExecQueryRelayLocalInfile_DirectErr(t *testing.T) {
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
}

func TestExecQueryRelayLocalInfile_RelaySuccess(t *testing.T) {
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
	var requestPayload []byte
	result, err := c.ExecQueryRelayLocalInfile("LOAD DATA LOCAL INFILE 'test.csv'", func(payload []byte) (io.Reader, error) {
		requestPayload = append([]byte(nil), payload...)
		return bytes.NewReader([]byte("row1\n")), nil
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, mysql.LocalInFile_HEADER, requestPayload[0])
	require.Equal(t, "test.csv", string(requestPayload[1:]))
}

func TestExecQueryRelayLocalInfile_RelayError(t *testing.T) {
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
}
