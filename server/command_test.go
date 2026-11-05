package server

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mocks"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// Ensure EmptyHandler implements Handler interface or cause compile time error
var (
	_ Handler            = EmptyHandler{}
	_ ReplicationHandler = EmptyReplicationHandler{}
)

func TestEmptyCommandPacket(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	srv := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	authHandler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, authHandler.AddUser("root", "secret"))
	// The empty packet must not reach the handler: a call to the mock fails.
	handler := &mocks.Handler{}

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		c, err := srv.NewCustomizedConn(conn, authHandler, handler)
		if err != nil {
			return
		}
		for {
			if err := c.HandleCommand(); err != nil {
				return
			}
		}
	}()

	conn, err := client.Connect(l.Addr().String(), "root", "secret", "")
	require.NoError(t, err)
	defer conn.Close()

	// A zero-length command packet gets ER_UNKNOWN_COM_ERROR...
	conn.ResetSequence()
	require.NoError(t, conn.WritePacket(make([]byte, 4)))
	data, err := conn.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, mysql.ERR_HEADER, data[0])
	require.EqualValues(t, mysql.ER_UNKNOWN_COM_ERROR, binary.LittleEndian.Uint16(data[1:]))

	// ...and the connection keeps working.
	require.NoError(t, conn.Ping())
}
