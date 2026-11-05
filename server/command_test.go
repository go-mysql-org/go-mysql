package server

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// Ensure EmptyHandler implements Handler interface or cause compile time error
var (
	_ Handler            = EmptyHandler{}
	_ ReplicationHandler = EmptyReplicationHandler{}
)

func TestDispatchEmptyPacket(t *testing.T) {
	c := &Conn{h: EmptyHandler{}}

	err, ok := c.dispatch(nil).(*mysql.MyError)
	require.True(t, ok)
	require.EqualValues(t, mysql.ER_UNKNOWN_COM_ERROR, err.Code)
}
