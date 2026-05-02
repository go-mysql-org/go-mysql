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
//
// Several caching_sha2 cases below are specifically chosen to be the
// "panic-shaped" inputs that would trip auth.CheckHashingPassword in
// upstream tidb (e.g. parts[3][:SALT_LENGTH] on a too-short final
// segment). validateHashedPassword's <= cachingSha2SaltLen check is what
// keeps that panic from being reachable through the documented API.
func TestAddUserWithHashedPasswordRejectsBadFormat(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)

	type tc struct {
		name   string
		plugin string
		hash   []byte
	}
	// Helpers for synthesizing inputs at exact byte lengths so the test
	// reads obviously rather than counting characters in literals.
	mkBytes := func(n int, fill byte) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = fill
		}
		return out
	}
	cs2OK := append(append([]byte("$A$005$"), mkBytes(cachingSha2SaltLen, 's')...), mkBytes(cachingSha2HashLen, 'h')...)
	_ = cs2OK // sanity: this exact shape must be accepted (covered by TestValidateHashedPassword_AcceptsRealVerifierOutput)

	cases := []tc{
		// mysql_native_password: must be exactly 20 bytes (SHA1×2).
		{"native: nil", mysql.AUTH_NATIVE_PASSWORD, nil},
		{"native: empty", mysql.AUTH_NATIVE_PASSWORD, []byte{}},
		{"native: 19 bytes", mysql.AUTH_NATIVE_PASSWORD, make([]byte, 19)},
		{"native: 21 bytes", mysql.AUTH_NATIVE_PASSWORD, make([]byte, 21)},
		{"native: hex string passed as bytes", mysql.AUTH_NATIVE_PASSWORD, []byte("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9")},

		// caching_sha2_password: "$A$<iter-hex>$<salt><hash>" — final
		// segment is exactly SALT_LENGTH(20) + 43 = 63 bytes. Anything
		// else is either store-format mismatch or panic-shaped input.
		{"caching_sha2: empty", mysql.AUTH_CACHING_SHA2_PASSWORD, []byte{}},
		{"caching_sha2: missing $A$ prefix", mysql.AUTH_CACHING_SHA2_PASSWORD, []byte("not-a-hash")},
		// 4-part split with the wrong digest type letter. parts[3] is
		// the right length so we know the test is exercising the type
		// check, not the length or part-count check.
		{"caching_sha2: wrong hash type 'B'", mysql.AUTH_CACHING_SHA2_PASSWORD, append(append([]byte("$B$005$"), mkBytes(cachingSha2SaltLen, 's')...), mkBytes(cachingSha2HashLen, 'h')...)},
		// Iter field is non-empty but contains a non-hex char.
		{"caching_sha2: non-hex iterations", mysql.AUTH_CACHING_SHA2_PASSWORD, append(append([]byte("$A$XYZ$"), mkBytes(cachingSha2SaltLen, 's')...), mkBytes(cachingSha2HashLen, 'h')...)},

		// 4-part split, type 'A', iter present, but parts[3] is exactly
		// SALT_LENGTH bytes — salt only, no hash tail. The verifier
		// would slice parts[3][:SALT_LENGTH] into the salt and run the
		// equality check against an empty hash, but it wouldn't panic.
		// We still reject because the resulting credential could never
		// authenticate (the verifier always rebuilds salt+43 bytes).
		{"caching_sha2: parts[3] salt-only, no hash tail", mysql.AUTH_CACHING_SHA2_PASSWORD, append([]byte("$A$005$"), mkBytes(cachingSha2SaltLen, 's')...)},
		// 4-part split, type 'A', iter present, but parts[3] is shorter
		// than SALT_LENGTH (1 byte). This is the actual panic shape:
		// without the length check, auth.CheckHashingPassword's
		// parts[3][:SALT_LENGTH] would slice past the end and panic.
		{"caching_sha2: parts[3] shorter than SALT_LENGTH (panic shape)", mysql.AUTH_CACHING_SHA2_PASSWORD, []byte("$A$005$x")},
		// Off-by-one in the final segment: one byte short and one byte
		// over the exact 63-byte salt+hash length.
		{"caching_sha2: parts[3] one byte short", mysql.AUTH_CACHING_SHA2_PASSWORD, append(append([]byte("$A$005$"), mkBytes(cachingSha2SaltLen, 's')...), mkBytes(cachingSha2HashLen-1, 'h')...)},
		{"caching_sha2: parts[3] one byte over", mysql.AUTH_CACHING_SHA2_PASSWORD, append(append([]byte("$A$005$"), mkBytes(cachingSha2SaltLen, 's')...), mkBytes(cachingSha2HashLen+1, 'h')...)},

		// sha256_password: "$<iter-decimal>$<salt:16>$<hashHex:64>".
		{"sha256: empty", mysql.AUTH_SHA256_PASSWORD, []byte{}},
		{"sha256: not a hash", mysql.AUTH_SHA256_PASSWORD, []byte("plain-string")},
		{"sha256: non-numeric iterations", mysql.AUTH_SHA256_PASSWORD, append(append([]byte("$A$"), mkBytes(mysql.SALT_LENGTH, 's')...), append([]byte("$"), mkBytes(sha256HashHexLen, 'a')...)...)},
		// Salt must be exactly mysql.SALT_LENGTH (16) — verifier slices
		// parts[2][:SALT_LENGTH] and ignores trailing bytes, so anything
		// other than exactly 16 is a store-format mismatch.
		{"sha256: salt one byte short (panic shape)", mysql.AUTH_SHA256_PASSWORD, append(append([]byte("$5$"), mkBytes(mysql.SALT_LENGTH-1, 's')...), append([]byte("$"), mkBytes(sha256HashHexLen, 'a')...)...)},
		{"sha256: salt one byte over", mysql.AUTH_SHA256_PASSWORD, append(append([]byte("$5$"), mkBytes(mysql.SALT_LENGTH+1, 's')...), append([]byte("$"), mkBytes(sha256HashHexLen, 'a')...)...)},
		// Hash segment must be exactly 64 hex chars.
		{"sha256: hash 63 chars", mysql.AUTH_SHA256_PASSWORD, append(append([]byte("$5$"), mkBytes(mysql.SALT_LENGTH, 's')...), append([]byte("$"), mkBytes(sha256HashHexLen-1, 'a')...)...)},
		{"sha256: hash 64 non-hex chars", mysql.AUTH_SHA256_PASSWORD, append(append([]byte("$5$"), mkBytes(mysql.SALT_LENGTH, 's')...), append([]byte("$"), mkBytes(sha256HashHexLen, 'z')...)...)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := handler.AddUserWithHashedPassword("bob", c.hash, c.plugin)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid hashed password")
		})
	}
}

// TestValidateHashedPassword_AcceptsRealVerifierOutput is the inverse of
// TestAddUserWithHashedPasswordRejectsBadFormat: it round-trips the
// upstream hash producers (auth.NewHashPassword for caching_sha2,
// mysql.NewSha256PasswordHash for sha256, mysql.NativePasswordHash for
// native) through validateHashedPassword and asserts each is accepted.
// This keeps the tightening from drifting away from what the verifier
// actually emits.
func TestValidateHashedPassword_AcceptsRealVerifierOutput(t *testing.T) {
	require.NoError(t, validateHashedPassword(
		mysql.AUTH_NATIVE_PASSWORD,
		mysql.NativePasswordHash([]byte("s3cr3t")),
	))
	require.NoError(t, validateHashedPassword(
		mysql.AUTH_CACHING_SHA2_PASSWORD,
		[]byte(auth.NewHashPassword("s3cr3t", mysql.AUTH_CACHING_SHA2_PASSWORD)),
	))
	storedSha, err := mysql.NewSha256PasswordHash("s3cr3t")
	require.NoError(t, err)
	require.NoError(t, validateHashedPassword(
		mysql.AUTH_SHA256_PASSWORD,
		[]byte(storedSha),
	))
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
	stored := cred.HashedPasswords[0].Bytes()
	require.NotEqual(t, hash, stored)
	require.Equal(t, mysql.NativePasswordHash([]byte(plaintext)), stored)
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
	require.NoError(t, handler.AddUserHashed("alice", hp))
	require.NoError(t, handler.AddUserHashed("bob", hp))

	for _, name := range []string{"alice", "bob"} {
		cred, found, err := handler.GetCredential(name)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, mysql.AUTH_NATIVE_PASSWORD, cred.AuthPluginName)
		require.Len(t, cred.HashedPasswords, 1)
		require.Equal(t, hash, cred.HashedPasswords[0].Bytes())
	}

	// HashedPassword's bytes are unreachable from outside the package
	// (unexported field, Bytes returns a copy), so the only mutable
	// surface a caller has on the returned credential is the outer
	// slice itself. Replacing the entry through the returned copy must
	// not affect the stored value, because GetCredential returns a
	// shallow clone of that slice.
	cred, _, _ := handler.GetCredential("alice")
	cred.HashedPasswords[0] = HashedPassword{}
	aliceCred2, _, _ := handler.GetCredential("alice")
	require.Len(t, aliceCred2.HashedPasswords, 1)
	require.Equal(t, hash, aliceCred2.HashedPasswords[0].Bytes())
	bobCred, _, _ := handler.GetCredential("bob")
	require.Equal(t, hash, bobCred.HashedPasswords[0].Bytes())
}

// TestGetCredentialReturnsDeepCopy pins the invariant that GetCredential
// returns a value the caller can freely mutate without affecting the stored
// credential. With []HashedPassword the inner bytes are unreachable for
// mutation already, so this test focuses on the two mutable surfaces a
// caller does have: the Passwords slice elements and the HashedPasswords
// slice elements.
func TestGetCredentialReturnsDeepCopy(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, handler.AddUser("alice", "p1"))
	require.NoError(t, handler.AddUserWithHashedPassword("bob", mysql.NativePasswordHash([]byte("p2"))))

	for _, name := range []string{"alice", "bob"} {
		first, _, err := handler.GetCredential(name)
		require.NoError(t, err)
		for i := range first.Passwords {
			first.Passwords[i] = "TAINTED"
		}
		for i := range first.HashedPasswords {
			first.HashedPasswords[i] = HashedPassword{}
		}
		second, _, err := handler.GetCredential(name)
		require.NoError(t, err)
		// Only compare fields that were actually mutated above. A user
		// registered via AddUser has no HashedPasswords entry; a user
		// registered via AddUserWithHashedPassword has no Passwords entry.
		if len(first.Passwords) > 0 {
			require.NotEqual(t, first.Passwords, second.Passwords, "%s: stored Passwords leaked through GetCredential", name)
		}
		if len(first.HashedPasswords) > 0 {
			require.NotEqual(t, first.HashedPasswords[0], second.HashedPasswords[0], "%s: stored HashedPasswords leaked through GetCredential", name)
		}
	}
}

// TestAppendUserHashed verifies the rotation path: an existing user can
// hold multiple HashedPassword entries, both of which authenticate, and
// the helper rejects mismatched plugin / unknown user inputs.
func TestAppendUserHashed(t *testing.T) {
	hashOld := mysql.NativePasswordHash([]byte("old"))
	hashNew := mysql.NativePasswordHash([]byte("new"))
	hpOld, err := NewHashedPassword(mysql.AUTH_NATIVE_PASSWORD, hashOld)
	require.NoError(t, err)
	hpNew, err := NewHashedPassword(mysql.AUTH_NATIVE_PASSWORD, hashNew)
	require.NoError(t, err)

	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, handler.AddUserHashed("alice", hpOld))
	require.NoError(t, handler.AppendUserHashed("alice", hpNew))

	cred, found, err := handler.GetCredential("alice")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, cred.HashedPasswords, 2, "rotation should retain both hashes")
	require.Equal(t, hashOld, cred.HashedPasswords[0].Bytes())
	require.Equal(t, hashNew, cred.HashedPasswords[1].Bytes())

	// Unknown user must not be auto-created.
	err = handler.AppendUserHashed("ghost", hpNew)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	// Plugin mismatch on an existing user is rejected.
	storedSha, err := mysql.NewSha256PasswordHash("anything")
	require.NoError(t, err)
	hpSha, err := NewHashedPassword(mysql.AUTH_SHA256_PASSWORD, []byte(storedSha))
	require.NoError(t, err)
	err = handler.AppendUserHashed("alice", hpSha)
	require.Error(t, err)
	require.Contains(t, err.Error(), "existing credential uses")
}

// TestAddUserHashedRejectsZeroValue covers the Finding #5 invariant:
// passing the zero-value HashedPassword (empty plugin, nil data) must
// return an error rather than silently registering a user that can
// never authenticate but still surfaces as "found" via hasAnyCredential.
func TestAddUserHashedRejectsZeroValue(t *testing.T) {
	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)

	err := handler.AddUserHashed("ghost", HashedPassword{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid hashed password")

	// And the user is not in the pool: GetCredential should report not-found.
	_, found, err := handler.GetCredential("ghost")
	require.NoError(t, err)
	require.False(t, found, "zero-value HashedPassword must not register the user")
}

// TestAddUserWithHashedPassword_CachingSha2 runs the same end-to-end
// shape as TestAddUserWithHashedPassword, but for caching_sha2_password.
// We use auth.NewHashPassword to produce the exact stored form a real
// MySQL server would have at rest, so this also documents the format
// callers should pass in. Because caching_sha2's full-auth flow sends
// the plaintext on the wire, the server must be configured with TLS
// (or an RSA key); we use the same tlsConf as the other server tests.
//
// The test exercises both auth paths separately:
//
//  1. First connection with the matching stored hash → cache miss,
//     full-auth path runs, and the server populates
//     cacheShaPassword with SHA256(SHA256(plaintext)).
//  2. We then swap the stored hash to one that does NOT correspond to
//     `plaintext`. A second connection with the original plaintext can
//     therefore only succeed via the fast-auth path: the swapped hash
//     would fail full-auth, so a successful Ping is positive evidence
//     of a cache hit.
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

	addr := l.Addr().String()

	// Phase 1: first connection with the matching stored hash. This
	// runs the full-auth path and populates the server's cache.
	dbFull, err := sql.Open("mysql", "alice:"+plaintext+"@tcp("+addr+")/test?tls=skip-verify")
	require.NoError(t, err)
	defer dbFull.Close()
	dbFull.SetConnMaxLifetime(time.Second)
	require.NoError(t, dbFull.Ping())
	// Drop the connection so the second Ping below cannot just reuse it
	// and skip authentication entirely.
	require.NoError(t, dbFull.Close())

	// Phase 2: rotate the stored hash to one that does NOT correspond
	// to `plaintext`. Full-auth from now on should fail; only the
	// cached SHA256(SHA256(plaintext)) can let the original password
	// authenticate.
	otherStored := []byte(auth.NewHashPassword("different-plaintext", mysql.AUTH_CACHING_SHA2_PASSWORD))
	require.NoError(t, handler.AddUserWithHashedPassword("alice", otherStored, mysql.AUTH_CACHING_SHA2_PASSWORD))

	// Phase 3: a fresh connection with the original plaintext must
	// still succeed — the only way that's possible now is via the
	// fast-auth cache hit populated in Phase 1.
	dbFast, err := sql.Open("mysql", "alice:"+plaintext+"@tcp("+addr+")/test?tls=skip-verify")
	require.NoError(t, err)
	defer dbFast.Close()
	dbFast.SetConnMaxLifetime(time.Second)
	dbFast.SetMaxIdleConns(0) // belt-and-braces: never reuse a pooled conn
	require.NoError(t, dbFast.Ping(), "fast-auth cache hit should succeed even though stored hash no longer matches plaintext")

	// Wrong plaintext → access denied (full-auth fails on otherStored,
	// and the cache scrambleValidation fails on a wrong client scramble
	// so it falls through to full-auth too).
	dbBad, err := sql.Open("mysql", "alice:wrongpass@tcp("+addr+")/test?tls=skip-verify")
	require.NoError(t, err)
	defer dbBad.Close()
	dbBad.SetConnMaxLifetime(time.Second)
	dbBad.SetMaxIdleConns(0)
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

// TestSafeVerifierWrappersRecover pins the Finding #2 invariant: the
// recover-safe wrappers around the upstream verifier calls must convert
// any slice-out-of-bounds panic on a malformed stored hash into a
// boring (false, error) return, so a Credential constructed directly
// (bypassing validateHashedPassword) cannot kill the connection
// goroutine.
func TestSafeVerifierWrappersRecover(t *testing.T) {
	t.Run("safeSha256Check on parts[2] shorter than SALT_LENGTH", func(t *testing.T) {
		// 4 $-segments, decimal iterations, but salt is 1 byte so the
		// upstream verifier's parts[2][:SALT_LENGTH] would slice past the
		// end and panic.
		bad := []byte("$5$x$" + // iter=5, salt="x", hash=""
			"deadbeef")
		match, err := safeSha256Check(bad, "anything")
		require.False(t, match)
		require.Error(t, err)
	})
	t.Run("safeCachingSha2Check on parts[3] shorter than SALT_LENGTH", func(t *testing.T) {
		// $A$, hex iter, then a final segment well below SALT_LENGTH.
		bad := []byte("$A$005$x")
		match, err := safeCachingSha2Check(bad, "anything", mysql.AUTH_CACHING_SHA2_PASSWORD)
		require.False(t, match)
		require.Error(t, err)
	})
	t.Run("safeNativeCompare on undersized hash", func(t *testing.T) {
		// Native verifier is permissive about input shapes (it doesn't
		// fixed-length-slice), so this case is mostly here to document
		// that the wrapper exists and never panics regardless of input.
		// An empty hash is enough to demonstrate "no panic, no match".
		match, err := safeNativeCompare([]byte("client-data"), nil, make([]byte, 20))
		require.False(t, match)
		// err may be nil if the verifier itself doesn't panic on this
		// shape; the contract is "no panic", not "always errors".
		_ = err
	})
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
