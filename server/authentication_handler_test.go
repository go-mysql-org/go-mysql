package server

import (
	"database/sql"
	"net"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-mysql-org/go-mysql/driver"
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
		rejectOnSuccess:               true,
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

// TestAddUserWithHashedPassword verifies the in-memory handler accepts a
// pre-computed mysql_native_password hash, that a client supplying the
// corresponding plaintext successfully authenticates, and that supplying a
// different plaintext fails — without the handler ever seeing the plaintext.
func TestAddUserWithHashedPassword(t *testing.T) {
	const plaintext = "s3cr3t"
	hash := mysql.NativePasswordHash([]byte(plaintext))

	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, handler.AddUserWithHashedPassword("alice", hash))

	// Round-trip: callers usually have the standard "*XXXX..." 41-char form
	// (e.g. from MySQL's mysql.user table or ProxySQL config). DecodePasswordHex
	// strips the leading '*' and returns the same 20 bytes.
	hexForm := mysql.EncodePasswordHex(hash)
	decoded, err := mysql.DecodePasswordHex(hexForm)
	require.NoError(t, err)
	require.Equal(t, hash, decoded)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		for {
			conn, acceptErr := l.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				co, _ := NewDefaultServer().NewCustomizedConn(c, handler, &EmptyHandler{})
				if co != nil {
					for co.HandleCommand() == nil {
					}
				}
			}(conn)
		}
	}()

	// Correct plaintext → server hashes the client's challenge response
	// and matches it against the stored HashedPasswords entry. The handler
	// never knows the plaintext.
	dbOK, err := sql.Open("mysql", "alice:"+plaintext+"@tcp("+l.Addr().String()+")/test")
	require.NoError(t, err)
	defer dbOK.Close()
	dbOK.SetConnMaxLifetime(time.Second)
	require.NoError(t, dbOK.Ping())

	// Wrong plaintext → access denied.
	dbBad, err := sql.Open("mysql", "alice:wrongpass@tcp("+l.Addr().String()+")/test")
	require.NoError(t, err)
	defer dbBad.Close()
	dbBad.SetConnMaxLifetime(time.Second)
	require.Error(t, dbBad.Ping())
}

// TestAddUserWithHashedPasswordRejectsUnsupportedPlugin confirms the helper
// fails up front for plugins that don't yet consume HashedPasswords, rather
// than silently accepting an unauthenticatable user.
func TestAddUserWithHashedPasswordRejectsUnsupportedPlugin(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	someHash := mysql.NativePasswordHash([]byte("anything"))

	err := handler.AddUserWithHashedPassword("bob", someHash, mysql.AUTH_CACHING_SHA2_PASSWORD)
	require.Error(t, err)
	require.Contains(t, err.Error(), "AddUserWithHashedPassword does not yet support")
}

func TestAddUserWithHashedPasswordRejectsEmptyHash(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	err := handler.AddUserWithHashedPassword("bob", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must not be empty")
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
