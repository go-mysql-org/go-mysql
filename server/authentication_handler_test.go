package server

import (
	"database/sql"
	"net"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/test_util/test_keys"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
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

// TestAddUserWithHashedPasswordRejectsUnknownPlugin confirms the helper
// fails up front for an unknown auth plugin name (typo, deprecated, etc.)
// rather than registering an unauthenticatable user.
func TestAddUserWithHashedPasswordRejectsUnknownPlugin(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	someHash := mysql.NativePasswordHash([]byte("anything"))

	err := handler.AddUserWithHashedPassword("bob", someHash, "made_up_auth_plugin")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown authentication plugin")
}

// TestAddUserWithHashedPasswordRejectsClearPassword confirms that
// mysql_clear_password — which has no meaningful hashed form — is
// directed to the plaintext API instead.
func TestAddUserWithHashedPasswordRejectsClearPassword(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	err := handler.AddUserWithHashedPassword("bob", []byte("anything"), mysql.AUTH_CLEAR_PASSWORD)
	require.Error(t, err)
	require.Contains(t, err.Error(), "use AddUser with the plaintext")
}

// TestAddUserWithHashedPasswordRejectsBadFormat covers the per-plugin
// shape validation: each plugin has a distinct stored-hash format, and
// passing a value that obviously doesn't match should fail at AddUser
// time rather than producing a user that can never authenticate.
func TestAddUserWithHashedPasswordRejectsBadFormat(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)

	type tc struct {
		name   string
		plugin string
		hash   []byte
	}
	cases := []tc{
		// mysql_native_password: must be exactly 20 bytes (SHA1×2).
		{"native: nil", mysql.AUTH_NATIVE_PASSWORD, nil},
		{"native: empty", mysql.AUTH_NATIVE_PASSWORD, []byte{}},
		{"native: 19 bytes", mysql.AUTH_NATIVE_PASSWORD, make([]byte, 19)},
		{"native: 21 bytes", mysql.AUTH_NATIVE_PASSWORD, make([]byte, 21)},
		{"native: hex string passed as bytes", mysql.AUTH_NATIVE_PASSWORD, []byte("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9")},

		// caching_sha2_password: "$A$<iter>$<salt>$<hash>" — wrong type or shape rejected.
		{"caching_sha2: empty", mysql.AUTH_CACHING_SHA2_PASSWORD, []byte{}},
		{"caching_sha2: missing $A$ prefix", mysql.AUTH_CACHING_SHA2_PASSWORD, []byte("not-a-hash")},
		{"caching_sha2: wrong hash type 'B'", mysql.AUTH_CACHING_SHA2_PASSWORD, []byte("$B$005$saltsaltsaltsaltsalt$hashhashhashhashhashhashhashhashhashhash43")},
		{"caching_sha2: not enough $-parts", mysql.AUTH_CACHING_SHA2_PASSWORD, []byte("$A$005$saltsaltsaltsaltsalt")},

		// sha256_password: "$<iter>$<salt>$<hashHex>" — iterations must be decimal.
		{"sha256: empty", mysql.AUTH_SHA256_PASSWORD, []byte{}},
		{"sha256: not a hash", mysql.AUTH_SHA256_PASSWORD, []byte("plain-string")},
		{"sha256: non-numeric iterations", mysql.AUTH_SHA256_PASSWORD, []byte("$A$saltsaltsaltsaltsalt$hashhashhashhashhashhashhashhash")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := handler.AddUserWithHashedPassword("bob", c.hash, c.plugin)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid hashed password")
		})
	}
}

// TestAddUserWithHashedPasswordCopiesSlice verifies the helper takes a
// defensive copy of the hash, so a caller that reuses or mutates its
// backing slice can't change the stored credential afterwards.
func TestAddUserWithHashedPasswordCopiesSlice(t *testing.T) {
	plaintext := "s3cr3t"
	hash := mysql.NativePasswordHash([]byte(plaintext))

	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, handler.AddUserWithHashedPassword("alice", hash))

	// Mutate the caller's slice. The stored credential must not change.
	for i := range hash {
		hash[i] = 0xff
	}

	cred, found, err := handler.GetCredential("alice")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, cred.HashedPasswords, 1)
	require.NotEqual(t, hash, cred.HashedPasswords[0])
	require.Equal(t, mysql.NativePasswordHash([]byte(plaintext)), cred.HashedPasswords[0])
}

// TestNewHashedPassword covers the construction-time validation and the
// defensive copy of the input hash bytes. These are the two guarantees the
// type promises beyond a raw (plugin, []byte) pair.
func TestNewHashedPassword(t *testing.T) {
	t.Run("native_password roundtrip", func(t *testing.T) {
		hash := mysql.NativePasswordHash([]byte("s3cr3t"))
		hp, err := NewHashedPassword(mysql.AUTH_NATIVE_PASSWORD, hash)
		require.NoError(t, err)
		require.Equal(t, mysql.AUTH_NATIVE_PASSWORD, hp.Plugin())
		require.Equal(t, hash, hp.Bytes())
	})

	t.Run("input slice is copied", func(t *testing.T) {
		hash := mysql.NativePasswordHash([]byte("s3cr3t"))
		original := append([]byte(nil), hash...)
		hp, err := NewHashedPassword(mysql.AUTH_NATIVE_PASSWORD, hash)
		require.NoError(t, err)
		// Mutate the caller's slice; the wrapped value must not change.
		for i := range hash {
			hash[i] = 0xff
		}
		require.Equal(t, original, hp.Bytes())
	})

	t.Run("Bytes returns a copy", func(t *testing.T) {
		hash := mysql.NativePasswordHash([]byte("s3cr3t"))
		hp, err := NewHashedPassword(mysql.AUTH_NATIVE_PASSWORD, hash)
		require.NoError(t, err)
		out := hp.Bytes()
		for i := range out {
			out[i] = 0xff
		}
		require.Equal(t, hash, hp.Bytes())
	})

	t.Run("rejects unknown plugin", func(t *testing.T) {
		_, err := NewHashedPassword("not_a_real_plugin", []byte{0x01})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown authentication plugin")
	})

	t.Run("rejects clear password", func(t *testing.T) {
		_, err := NewHashedPassword(mysql.AUTH_CLEAR_PASSWORD, []byte("plaintext"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "use AddUser with the plaintext")
	})

	t.Run("rejects bad shape", func(t *testing.T) {
		_, err := NewHashedPassword(mysql.AUTH_NATIVE_PASSWORD, []byte{0x00, 0x01})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid hashed password length")
	})
}

// TestAddUserHashed verifies the constructor-based registration path
// stores the credential and is decoupled from the caller's input slice
// even when the same HashedPassword is reused across multiple users.
func TestAddUserHashed(t *testing.T) {
	hash := mysql.NativePasswordHash([]byte("s3cr3t"))
	hp, err := NewHashedPassword(mysql.AUTH_NATIVE_PASSWORD, hash)
	require.NoError(t, err)

	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	handler.AddUserHashed("alice", hp)
	handler.AddUserHashed("bob", hp)

	for _, name := range []string{"alice", "bob"} {
		cred, found, err := handler.GetCredential(name)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, mysql.AUTH_NATIVE_PASSWORD, cred.AuthPluginName)
		require.Len(t, cred.HashedPasswords, 1)
		require.Equal(t, hash, cred.HashedPasswords[0])
	}

	// Mutating one stored credential's slice must not bleed into the other:
	// AddUserHashed clones on insert so the two users don't share storage.
	cred, _, _ := handler.GetCredential("alice")
	for i := range cred.HashedPasswords[0] {
		cred.HashedPasswords[0][i] = 0xff
	}
	bobCred, _, _ := handler.GetCredential("bob")
	require.Equal(t, hash, bobCred.HashedPasswords[0])
}

// TestAddUserWithHashedPassword_CachingSha2 runs the same end-to-end
// shape as TestAddUserWithHashedPassword, but for caching_sha2_password.
// We use auth.NewHashPassword to produce the exact stored form a real
// MySQL server would have at rest, so this also documents the format
// callers should pass in. Because caching_sha2's full-auth flow sends
// the plaintext on the wire, the server must be configured with TLS
// (or an RSA key); we use the same tlsConf as the other server tests.
func TestAddUserWithHashedPassword_CachingSha2(t *testing.T) {
	const plaintext = "s3cr3t"
	stored := []byte(auth.NewHashPassword(plaintext, mysql.AUTH_CACHING_SHA2_PASSWORD))

	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_CACHING_SHA2_PASSWORD)
	require.NoError(t, handler.AddUserWithHashedPassword("alice", stored, mysql.AUTH_CACHING_SHA2_PASSWORD))

	srv := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, test_keys.RSAKey(), tlsConf)

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
				co, _ := srv.NewCustomizedConn(c, handler, &EmptyHandler{})
				if co != nil {
					for co.HandleCommand() == nil {
					}
				}
			}(conn)
		}
	}()

	// Correct plaintext under TLS → success. Two pings exercise both the
	// full-auth path (cache miss) and the fast-auth path (cache hit).
	dbOK, err := sql.Open("mysql", "alice:"+plaintext+"@tcp("+l.Addr().String()+")/test?tls=skip-verify")
	require.NoError(t, err)
	defer dbOK.Close()
	dbOK.SetConnMaxLifetime(time.Second)
	require.NoError(t, dbOK.Ping())
	require.NoError(t, dbOK.Ping())

	// Wrong plaintext → access denied.
	dbBad, err := sql.Open("mysql", "alice:wrongpass@tcp("+l.Addr().String()+")/test?tls=skip-verify")
	require.NoError(t, err)
	defer dbBad.Close()
	dbBad.SetConnMaxLifetime(time.Second)
	require.Error(t, dbBad.Ping())
}

// TestAddUserWithHashedPassword_Sha256Password mirrors the test above for
// sha256_password. Same TLS requirement; no cache layer.
func TestAddUserWithHashedPassword_Sha256Password(t *testing.T) {
	const plaintext = "s3cr3t"
	storedString, err := mysql.NewSha256PasswordHash(plaintext)
	require.NoError(t, err)
	stored := []byte(storedString)

	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_SHA256_PASSWORD)
	require.NoError(t, handler.AddUserWithHashedPassword("alice", stored, mysql.AUTH_SHA256_PASSWORD))

	srv := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, test_keys.RSAKey(), tlsConf)

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
				co, _ := srv.NewCustomizedConn(c, handler, &EmptyHandler{})
				if co != nil {
					for co.HandleCommand() == nil {
					}
				}
			}(conn)
		}
	}()

	dbOK, err := sql.Open("mysql", "alice:"+plaintext+"@tcp("+l.Addr().String()+")/test?tls=skip-verify")
	require.NoError(t, err)
	defer dbOK.Close()
	dbOK.SetConnMaxLifetime(time.Second)
	require.NoError(t, dbOK.Ping())

	dbBad, err := sql.Open("mysql", "alice:wrongpass@tcp("+l.Addr().String()+")/test?tls=skip-verify")
	require.NoError(t, err)
	defer dbBad.Close()
	dbBad.SetConnMaxLifetime(time.Second)
	require.Error(t, dbBad.Ping())
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
