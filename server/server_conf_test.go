package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/stretchr/testify/require"
)

var multiResultServerCapabilities = []uint32{
	mysql.CLIENT_MULTI_RESULTS,
	mysql.CLIENT_PS_MULTI_RESULTS,
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
	require.NoError(t, svr.SetCapability(mysql.CLIENT_LOCAL_FILES))
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

// TestLocalFilesCapabilityCanBeDisabled verifies that CLIENT_LOCAL_FILES is not advertised
// by default and is not negotiated unless explicitly enabled via SetCapability.
func TestLocalFilesCapabilityCanBeDisabled(t *testing.T) {
	svr := NewDefaultServer()
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

// TestFoundRowsCapabilityNegotiated verifies that CLIENT_FOUND_ROWS survives handshake
// negotiation once the server opts in via SetCapability. Clients that mask their
// requested capabilities against the server's advertised set are otherwise silently
// downgraded to changed-row UPDATE counts.
func TestFoundRowsCapabilityNegotiated(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	svr := NewDefaultServer()
	require.NoError(t, svr.SetCapability(mysql.CLIENT_FOUND_ROWS))
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
			return conn.SetCapability(mysql.CLIENT_FOUND_ROWS)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.Contains(t, negotiated, "CLIENT_FOUND_ROWS",
		"CLIENT_FOUND_ROWS must be negotiated for matched-row UPDATE counts, got: %s", negotiated)
}

// TestFoundRowsCapabilityCanBeDisabled verifies that CLIENT_FOUND_ROWS is not advertised
// by default and is not negotiated unless explicitly enabled via SetCapability. It stays
// opt-in because honoring it is the Handler's job.
func TestFoundRowsCapabilityCanBeDisabled(t *testing.T) {
	svr := NewDefaultServer()
	require.False(t, svr.Capability()&mysql.CLIENT_FOUND_ROWS != 0)

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
			return conn.SetCapability(mysql.CLIENT_FOUND_ROWS)
		},
	)
	require.NoError(t, err)
	defer c.Close()

	negotiated := c.CapabilityString()
	require.NotContains(t, negotiated, "CLIENT_FOUND_ROWS",
		"CLIENT_FOUND_ROWS must not be negotiated when the server does not advertise it, got: %s", negotiated)
}

func TestSetCapabilityRejectsUnsafeFlags(t *testing.T) {
	svr := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)

	err := svr.SetCapability(mysql.CLIENT_SSL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-user-configurable flags")
	require.Contains(t, err.Error(), fmt.Sprintf("%#x", mysql.CLIENT_SSL))
	require.False(t, svr.Capability()&mysql.CLIENT_SSL != 0)

	err = svr.SetCapability(mysql.CLIENT_LOCAL_FILES | mysql.CLIENT_SSL)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("%#x", mysql.CLIENT_SSL))
	require.False(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)

	err = svr.UnsetCapability(mysql.CLIENT_PROTOCOL_41)
	require.Error(t, err)
	require.True(t, svr.Capability()&mysql.CLIENT_PROTOCOL_41 != 0)

	require.NoError(t, svr.SetCapability(mysql.CLIENT_LOCAL_FILES))
	require.True(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)
	require.NoError(t, svr.UnsetCapability(mysql.CLIENT_LOCAL_FILES))
	require.False(t, svr.Capability()&mysql.CLIENT_LOCAL_FILES != 0)

	require.NoError(t, svr.SetCapability(mysql.CLIENT_FOUND_ROWS))
	require.True(t, svr.Capability()&mysql.CLIENT_FOUND_ROWS != 0)
	require.NoError(t, svr.UnsetCapability(mysql.CLIENT_FOUND_ROWS))
	require.False(t, svr.Capability()&mysql.CLIENT_FOUND_ROWS != 0)
}

func TestSetInitialStatusRejectsNonSessionFlags(t *testing.T) {
	svr := NewDefaultServer()
	require.Zero(t, svr.InitialStatus())
	for _, flag := range []uint16{mysql.SERVER_STATUS_IN_TRANS, mysql.SERVER_MORE_RESULTS_EXISTS, mysql.SERVER_SESSION_STATE_CHANGED} {
		require.Error(t, svr.SetInitialStatus(mysql.SERVER_STATUS_AUTOCOMMIT|flag))
		require.Zero(t, svr.InitialStatus(), "a rejected status must not be applied")
	}
	require.NoError(t, svr.SetInitialStatus(mysql.SERVER_STATUS_AUTOCOMMIT|mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED))
	require.Equal(t, mysql.SERVER_STATUS_AUTOCOMMIT|mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED, svr.InitialStatus())
	require.NoError(t, svr.SetInitialStatus(0))
	require.Zero(t, svr.InitialStatus())
}

// TestInitialStatusOnTheWire verifies that a connection reports the server's
// initial status before its Handler has run: in the initial handshake packet,
// in the OK that ends authentication, and in the reply to a first command.
func TestInitialStatusOnTheWire(t *testing.T) {
	for _, status := range []uint16{0, mysql.SERVER_STATUS_AUTOCOMMIT} {
		t.Run(fmt.Sprintf("%#04x", status), func(t *testing.T) {
			svr := NewDefaultServer()
			require.NoError(t, svr.SetInitialStatus(status))
			l, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer l.Close()
			authHandler := NewInMemoryAuthenticationHandler()
			require.NoError(t, authHandler.AddUser("root", ""))
			go func() {
				for {
					conn, acceptErr := l.Accept()
					if acceptErr != nil {
						return
					}
					go func() {
						sConn, connErr := svr.NewCustomizedConn(conn, authHandler, EmptyHandler{})
						if connErr != nil {
							return
						}
						for !sConn.Closed() {
							if sConn.HandleCommand() != nil {
								return
							}
						}
					}()
				}
			}()

			// The handshake packet: protocol version, NUL-terminated server
			// version, connection id, 8 bytes of auth data, filler, capability
			// low half, charset, then the status.
			raw, err := net.Dial("tcp", l.Addr().String())
			require.NoError(t, err)
			defer raw.Close()
			greeting, err := packet.NewConn(raw).ReadPacket()
			require.NoError(t, err)
			pos := 1 + bytes.IndexByte(greeting[1:], 0) + 1 + 4 + 8 + 1 + 2 + 1
			require.Equal(t, status, binary.LittleEndian.Uint16(greeting[pos:]), "initial handshake")

			c, err := client.Connect(l.Addr().String(), "root", "", "")
			require.NoError(t, err)
			defer c.Close()
			wantAutocommit := status&mysql.SERVER_STATUS_AUTOCOMMIT != 0
			require.Equal(t, wantAutocommit, c.IsAutoCommit(), "authentication OK")
			require.NoError(t, c.Ping())
			require.Equal(t, wantAutocommit, c.IsAutoCommit(), "COM_PING")
		})
	}
}
