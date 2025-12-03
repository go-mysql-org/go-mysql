package server

import (
	"database/sql"
	"net"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

type hookTrackingAuthenticationHandler struct {
	*InMemoryAuthenticationHandler
	onSuccessCalled atomic.Int32
	onFailureCalled atomic.Int32
	rejectOnSuccess bool
}

func (h *hookTrackingAuthenticationHandler) OnAuthSuccess(conn *Conn) error {
	h.onSuccessCalled.Add(1)
	if h.rejectOnSuccess {
		return errors.New("connection rejected by policy")
	}
	return nil
}

func (h *hookTrackingAuthenticationHandler) OnAuthFailure(conn *Conn, err error) {
	h.onFailureCalled.Add(1)
}

func TestOnAuthSuccessCalled(t *testing.T) {
	handler := &hookTrackingAuthenticationHandler{
		InMemoryAuthenticationHandler: NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD),
	}
	require.NoError(t, handler.AddUser("testuser", "testpass"))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, _ := l.Accept()
		co, _ := NewDefaultServer().NewCustomizedConn(conn, handler, &EmptyHandler{})
		if co != nil {
			for co.HandleCommand() == nil {
			}
		}
	}()

	db, err := sql.Open("mysql", "testuser:testpass@tcp("+l.Addr().String()+")/test")
	require.NoError(t, err)
	defer db.Close()
	db.SetConnMaxLifetime(time.Second)

	require.NoError(t, db.Ping())
	require.Equal(t, int32(1), handler.onSuccessCalled.Load())
	require.Equal(t, int32(0), handler.onFailureCalled.Load())
}

func TestOnAuthSuccessCanReject(t *testing.T) {
	handler := &hookTrackingAuthenticationHandler{
		InMemoryAuthenticationHandler: NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD),
		rejectOnSuccess:     true,
	}
	require.NoError(t, handler.AddUser("testuser", "testpass"))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, _ := l.Accept()
		co, _ := NewDefaultServer().NewCustomizedConn(conn, handler, &EmptyHandler{})
		if co != nil {
			for co.HandleCommand() == nil {
			}
		}
	}()

	db, err := sql.Open("mysql", "testuser:testpass@tcp("+l.Addr().String()+")/test")
	require.NoError(t, err)
	defer db.Close()
	db.SetConnMaxLifetime(time.Second)

	err = db.Ping()
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection rejected by policy")
	require.Equal(t, int32(1), handler.onSuccessCalled.Load())
}

func TestOnAuthFailureCalled(t *testing.T) {
	handler := &hookTrackingAuthenticationHandler{
		InMemoryAuthenticationHandler: NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD),
	}
	require.NoError(t, handler.AddUser("testuser", "testpass"))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, _ := l.Accept()
		co, _ := NewDefaultServer().NewCustomizedConn(conn, handler, &EmptyHandler{})
		if co != nil {
			for co.HandleCommand() == nil {
			}
		}
	}()

	db, err := sql.Open("mysql", "testuser:wrongpass@tcp("+l.Addr().String()+")/test")
	require.NoError(t, err)
	defer db.Close()
	db.SetConnMaxLifetime(time.Second)

	require.Error(t, db.Ping())
	require.Equal(t, int32(0), handler.onSuccessCalled.Load())
	require.Equal(t, int32(1), handler.onFailureCalled.Load())
}
