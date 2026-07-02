package server

import (
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

var multiResultServerCapabilities = []uint32{
	mysql.CLIENT_MULTI_RESULTS,
	mysql.CLIENT_PS_MULTI_RESULTS,
	mysql.CLIENT_LOCAL_FILES,
}

func TestDefaultServerCapabilities(t *testing.T) {
	s := NewDefaultServer()
	assertServerAdvertisesCapabilities(t, s)
}

func TestNewServerCapabilities(t *testing.T) {
	s := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	assertServerAdvertisesCapabilities(t, s)
}

func assertServerAdvertisesCapabilities(t *testing.T, s *Server) {
	t.Helper()
	for _, cap := range multiResultServerCapabilities {
		require.True(t, s.Capability()&cap != 0,
			"expected server to advertise %s", mysql.CapNames[cap])
	}
}

// procHandler simulates a proxy handler that forwards CALL statements to an upstream MySQL.
type procHandler struct{ EmptyHandler }

func (h *procHandler) HandleQuery(query string) (*mysql.Result, error) {
	if query != "CALL get_report()" {
		return nil, nil
	}

	// A stored procedure that returns result sets requires CLIENT_MULTI_RESULTS to be
	// negotiated end-to-end. Without it, a real MySQL upstream returns Error 1312:
	// "PROCEDURE get_report can't return a result set in the given context".
	rs, err := mysql.BuildSimpleResultset(
		[]string{"id", "name"},
		[][]any{{1, "alice"}, {2, "bob"}},
		false,
	)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

// TestNegotiatedMultiResultsCapability reproduces the stored-procedure CALL issue:
// JDBC and other clients request CLIENT_MULTI_RESULTS during handshake, but when the
// go-mysql server does not advertise it, the negotiated capability is cleared and
// CALL statements that return result sets fail on a real MySQL upstream (Error 1312).
func TestNegotiatedMultiResultsCapability(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	svr := NewDefaultServer()
	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("root", ""))

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr != nil {
			return
		}
		sConn, connErr := svr.NewCustomizedConn(conn, authHandler, &procHandler{})
		if connErr != nil {
			return
		}
		for {
			if handleErr := sConn.HandleCommand(); handleErr != nil {
				return
			}
		}
	}()

	// Simulate JDBC Connector/J: request multi-result support during handshake.
	c, err := client.Connect(l.Addr().String(), "root", "", "",
		func(conn *client.Conn) error {
			if err := conn.SetCapability(mysql.CLIENT_MULTI_RESULTS); err != nil {
				return err
			}
			return conn.SetCapability(mysql.CLIENT_PS_MULTI_RESULTS)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.Contains(t, negotiated, "CLIENT_MULTI_RESULTS",
		"CLIENT_MULTI_RESULTS must be negotiated for CALL stored procedures, got: %s", negotiated)
	require.Contains(t, negotiated, "CLIENT_PS_MULTI_RESULTS",
		"CLIENT_PS_MULTI_RESULTS must be negotiated for prepared CALL with OUT params, got: %s", negotiated)

	result, err := c.Execute("CALL get_report()")
	require.NoError(t, err)
	defer result.Close()
	require.True(t, result.HasResultset())
	require.Equal(t, 2, len(result.Values))
}

// TestLocalFilesCapabilityNegotiated verifies that CLIENT_LOCAL_FILES is advertised
// by the server and survives handshake negotiation when requested by the client.
func TestLocalFilesCapabilityNegotiated(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	svr := NewDefaultServer()
	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("root", ""))

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr != nil {
			return
		}
		sConn, connErr := svr.NewCustomizedConn(conn, authHandler, EmptyHandler{})
		if connErr != nil {
			return
		}
		for {
			if handleErr := sConn.HandleCommand(); handleErr != nil {
				return
			}
		}
	}()

	c, err := client.Connect(l.Addr().String(), "root", "", "",
		func(conn *client.Conn) error {
			return conn.SetCapability(mysql.CLIENT_LOCAL_FILES)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.Contains(t, negotiated, "CLIENT_LOCAL_FILES",
		"CLIENT_LOCAL_FILES must be negotiated for LOAD DATA LOCAL INFILE relay, got: %s", negotiated)
}

// TestLocalFilesCapabilityCanBeDisabled verifies that downstream users can opt out of
// CLIENT_LOCAL_FILES when they do not need LOAD DATA LOCAL INFILE relay support.
func TestLocalFilesCapabilityCanBeDisabled(t *testing.T) {
	svr := NewDefaultServer()
	svr.UnsetCapability(mysql.CLIENT_LOCAL_FILES)
	require.False(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("root", ""))

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr != nil {
			return
		}
		sConn, connErr := svr.NewCustomizedConn(conn, authHandler, EmptyHandler{})
		if connErr != nil {
			return
		}
		for {
			if handleErr := sConn.HandleCommand(); handleErr != nil {
				return
			}
		}
	}()

	c, err := client.Connect(l.Addr().String(), "root", "", "",
		func(conn *client.Conn) error {
			return conn.SetCapability(mysql.CLIENT_LOCAL_FILES)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.NotContains(t, negotiated, "CLIENT_LOCAL_FILES",
		"CLIENT_LOCAL_FILES must not be negotiated when the server does not advertise it, got: %s", negotiated)
}
