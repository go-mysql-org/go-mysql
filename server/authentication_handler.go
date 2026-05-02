package server

import (
	"bytes"
	"slices"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
)

// AuthenticationHandler provides user credentials and authentication lifecycle hooks.
//
// # Important Note
//
// if the password in a third-party auth handler could be updated at runtime, we have to invalidate the caching
// for 'caching_sha2_password' by calling 'func (s *Server)InvalidateCache(string, string)'.
type AuthenticationHandler interface {
	// GetCredential returns the user credential (supports multiple valid passwords per user).
	// Implementations must be safe for concurrent use.
	GetCredential(username string) (credential Credential, found bool, err error)

	// OnAuthSuccess is called after successful authentication, before the OK packet.
	// Return an error to reject the connection (error will be sent to client instead of OK).
	// Return nil to proceed with sending the OK packet.
	OnAuthSuccess(conn *Conn) error

	// OnAuthFailure is called after authentication fails, before the error packet.
	// This is informational only - the connection will be closed regardless.
	OnAuthFailure(conn *Conn, err error)
}

func NewInMemoryAuthenticationHandler(defaultAuthMethod ...string) *InMemoryAuthenticationHandler {
	d := mysql.AUTH_CACHING_SHA2_PASSWORD
	if len(defaultAuthMethod) > 0 {
		d = defaultAuthMethod[0]
	}
	return &InMemoryAuthenticationHandler{
		userPool:          sync.Map{},
		defaultAuthMethod: d,
	}
}

// Credential holds authentication settings for a user.
//
// Passwords contains all valid raw passwords for the user. They are hashed on demand during comparison.
// If empty password authentication is allowed, Passwords must contain an empty string (e.g., []string{""})
// rather than being a zero-length slice. A zero-length slice means no valid passwords are configured.
//
// HashedPasswords contains pre-computed password hashes that the server compares directly against the
// client's challenge response, without ever needing the plaintext. This lets callers (e.g. a MySQL proxy
// rehoming users from another server's `mysql.user` table) configure credentials when only the stored
// hash is available. The byte format depends on AuthPluginName:
//
//   - mysql_native_password: the 20-byte SHA1(SHA1(plaintext)) value, i.e. what
//     mysql.NativePasswordHash returns, or what mysql.DecodePasswordHex returns from MySQL's standard
//     "*XXXX..." (41-char) hex form.
//   - caching_sha2_password: the bytes of the standard "$A$<iter>$<salt><hash>" stored form (salt
//     and hash are concatenated, with no '$' between them — see auth.NewHashPassword in
//     pingcap/tidb). Note that this auth plugin's full-auth flow requires either TLS or a
//     configured RSA key on the server, because the server must obtain the plaintext to verify it
//     against the stored hash and to populate the cache. After the first successful full auth the
//     server caches SHA256(SHA256(plaintext)) per user@host so subsequent connections can take the
//     fast-auth path.
//   - sha256_password: the bytes of the standard "$<iter>$<salt>$<hashHex>" stored form, i.e. what
//     mysql.NewSha256PasswordHash returns. Same TLS-or-RSA requirement as caching_sha2_password,
//     for the same reason.
//
// Both fields can be set on the same Credential: HashedPasswords is checked first (cheaper, no
// hashing per connect), then Passwords.
//
// Hashes installed via AddUserWithHashedPassword / NewHashedPassword are
// shape-checked up front. Constructing a Credential directly and inserting
// arbitrary bytes into HashedPasswords bypasses that check: a malformed
// caching_sha2_password value (e.g. a final "$A$<iter>$<salt><hash>"
// segment shorter than the 20-byte salt) can panic inside the upstream
// tidb verifier auth.CheckHashingPassword. Callers loading hashes from an
// untrusted source should go through the documented helpers, or run their
// own validation before assigning to this field.
type Credential struct {
	Passwords       []string
	HashedPasswords [][]byte
	AuthPluginName  string
}

// HashedPassword pairs a stored-form password hash with its auth plugin and
// guarantees that the pair has been validated for shape (see
// validateHashedPassword) and that the bytes were defensively copied at
// construction time. Construct it with NewHashedPassword; the zero value is
// not usable.
//
// The point of the type is to let callers validate a (plugin, hash) pair once
// — for example when loading credentials at startup — and then reuse the
// resulting value, instead of revalidating on every AddUser call. See
// Credential.HashedPasswords for the per-plugin byte format.
type HashedPassword struct {
	plugin string
	data   []byte
}

// NewHashedPassword validates hash for the given auth plugin and returns a
// HashedPassword wrapping a defensive copy of the bytes. authPluginName must
// be one of the supported hash-bearing plugins (mysql_native_password,
// caching_sha2_password, sha256_password); mysql_clear_password has no
// hashed form and is rejected. See Credential.HashedPasswords for the
// expected byte layout per plugin.
func NewHashedPassword(authPluginName string, hash []byte) (HashedPassword, error) {
	if err := validateHashedPassword(authPluginName, hash); err != nil {
		return HashedPassword{}, err
	}
	return HashedPassword{plugin: authPluginName, data: slices.Clone(hash)}, nil
}

// Plugin returns the auth plugin name this hash was constructed for.
func (h HashedPassword) Plugin() string { return h.plugin }

// Bytes returns a copy of the stored hash bytes. The copy means callers can
// safely mutate the returned slice without affecting the HashedPassword.
func (h HashedPassword) Bytes() []byte { return slices.Clone(h.data) }

// hashPassword computes the password hash for a given password using the credential's auth plugin.
func (c Credential) hashPassword(password string) (string, error) {
	if password == "" {
		return "", nil
	}

	switch c.AuthPluginName {
	case mysql.AUTH_NATIVE_PASSWORD:
		return mysql.EncodePasswordHex(mysql.NativePasswordHash([]byte(password))), nil

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		return auth.NewHashPassword(password, mysql.AUTH_CACHING_SHA2_PASSWORD), nil

	case mysql.AUTH_SHA256_PASSWORD:
		return mysql.NewSha256PasswordHash(password)

	case mysql.AUTH_CLEAR_PASSWORD:
		return password, nil

	default:
		return "", errors.Errorf("unknown authentication plugin name '%s'", c.AuthPluginName)
	}
}

// hasEmptyPassword returns true if any password in the credential is empty.
func (c Credential) hasEmptyPassword() bool {
	return slices.Contains(c.Passwords, "")
}

// hasAnyCredential reports whether the credential has at least one usable
// entry — either a plaintext password (including the empty string, which
// signals "empty password is OK") or a pre-computed hash.
func (c Credential) hasAnyCredential() bool {
	return len(c.Passwords) > 0 || len(c.HashedPasswords) > 0
}

// cloneCredential returns a deep copy of c so that callers cannot mutate
// the stored slices. Both Passwords and the per-entry HashedPasswords byte
// slices are copied; AuthPluginName is a string and is already immutable.
func cloneCredential(c Credential) Credential {
	out := Credential{AuthPluginName: c.AuthPluginName}
	if c.Passwords != nil {
		out.Passwords = slices.Clone(c.Passwords)
	}
	if c.HashedPasswords != nil {
		out.HashedPasswords = make([][]byte, len(c.HashedPasswords))
		for i, h := range c.HashedPasswords {
			out.HashedPasswords[i] = slices.Clone(h)
		}
	}
	return out
}

// InMemoryAuthenticationHandler implements AuthenticationHandler with in-memory credential storage.
type InMemoryAuthenticationHandler struct {
	userPool          sync.Map // username -> Credential
	defaultAuthMethod string
}

func (h *InMemoryAuthenticationHandler) CheckUsername(username string) (found bool, err error) {
	_, ok := h.userPool.Load(username)
	return ok, nil
}

func (h *InMemoryAuthenticationHandler) GetCredential(username string) (credential Credential, found bool, err error) {
	v, ok := h.userPool.Load(username)
	if !ok {
		return Credential{}, false, nil
	}
	c, valid := v.(Credential)
	if !valid {
		return Credential{}, true, errors.Errorf("invalid credential")
	}
	// Defensive deep copy: Credential carries []string and [][]byte fields
	// whose backing arrays would otherwise be shared with the stored value.
	// A caller mutating the returned slices (intentionally or by accident)
	// must not corrupt the credential held in the user pool.
	return cloneCredential(c), true, nil
}

func (h *InMemoryAuthenticationHandler) AddUser(username, password string, optionalAuthPluginName ...string) error {
	authPluginName := h.defaultAuthMethod
	if len(optionalAuthPluginName) > 0 {
		authPluginName = optionalAuthPluginName[0]
	}

	if !isAuthMethodSupported(authPluginName) {
		return errors.Errorf("unknown authentication plugin name '%s'", authPluginName)
	}

	h.userPool.Store(username, Credential{
		Passwords:      []string{password},
		AuthPluginName: authPluginName,
	})
	return nil
}

// AddUserWithHashedPassword registers a user whose password is already in
// the server-side hashed form, so the plaintext never has to be supplied.
//
// The expected byte format depends on authPluginName; see Credential.HashedPasswords
// for the full list. As shorthand:
//
//   - mysql_native_password: the 20-byte SHA1(SHA1(plaintext)) value. Use
//     mysql.DecodePasswordHex to strip the "*" and decode MySQL's 41-char hex form.
//   - caching_sha2_password: the bytes of "$A$<iter>$<salt><hash>".
//   - sha256_password: the bytes of "$<iter>$<salt>$<hashHex>".
//
// The hash is rejected up front if it doesn't match the expected shape for
// the chosen auth plugin, so a misconfigured caller fails immediately rather
// than registering a user that can never authenticate. authPluginName
// defaults to the handler's default auth method.
//
// caching_sha2_password and sha256_password additionally require the server
// to be configured with TLS or an RSA key, because the full-auth flow needs
// the plaintext on the server side — both to verify against the stored hash
// and (for caching_sha2) to populate the cache. Same constraint that
// already applies to plaintext Passwords with these plugins.
//
// Example:
//
//	handler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
//	bytes, _ := mysql.DecodePasswordHex("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9")
//	handler.AddUserWithHashedPassword("alice", bytes)
func (h *InMemoryAuthenticationHandler) AddUserWithHashedPassword(username string, hash []byte, optionalAuthPluginName ...string) error {
	authPluginName := h.defaultAuthMethod
	if len(optionalAuthPluginName) > 0 {
		authPluginName = optionalAuthPluginName[0]
	}
	hp, err := NewHashedPassword(authPluginName, hash)
	if err != nil {
		return err
	}
	return h.AddUserHashed(username, hp)
}

// AddUserHashed registers username with a pre-validated HashedPassword.
// Equivalent to AddUserWithHashedPassword but takes the already-validated
// value from NewHashedPassword, which is convenient when the same hash is
// being installed against multiple usernames or when callers want to keep
// validation separate from registration.
//
// The HashedPassword zero value is rejected — its plugin name is empty and
// its data is nil, which would otherwise register a user that is reachable
// (`hasAnyCredential` returns true on `[][]byte{nil}`) but can never
// authenticate. To stay defensive against any in-place mutation between
// NewHashedPassword and this call, AddUserHashed re-runs
// validateHashedPassword on the wrapped (plugin, data) pair.
func (h *InMemoryAuthenticationHandler) AddUserHashed(username string, hp HashedPassword) error {
	if err := validateHashedPassword(hp.plugin, hp.data); err != nil {
		return err
	}
	// Defensive copy so a caller that constructed HashedPassword via
	// NewHashedPassword (which already cloned the input) is still
	// protected from later AddUserHashed calls accidentally aliasing
	// the same backing array, and so HashedPassword.Bytes returning a
	// fresh copy stays consistent with the credential storage model.
	h.userPool.Store(username, Credential{
		HashedPasswords: [][]byte{slices.Clone(hp.data)},
		AuthPluginName:  hp.plugin,
	})
	return nil
}

const (
	// mysqlNativePasswordHashLen is the length of a native_password hash
	// (sha1(sha1(plaintext))) in bytes.
	mysqlNativePasswordHashLen = 20
	// cachingSha2SaltLen is auth.SALT_LENGTH (the constant is unexported
	// in pingcap/tidb's parser/auth package). The salt is 20 bytes for
	// caching_sha2_password regardless of plaintext length.
	cachingSha2SaltLen = 20
	// cachingSha2HashLen is the length of the hash portion appended after
	// the salt in the final $A$<iter>$<salt><hash> segment, derived from
	// the eleven b64From24bit calls in auth.hashCrypt (10 × 4 chars + 1
	// × 3 chars).
	cachingSha2HashLen = 43
	// sha256HashHexLen is the length of the hash segment in
	// $<iter>$<salt>$<hashHex> for sha256_password — sha256 produces 32
	// bytes and hex.EncodeToString doubles that.
	sha256HashHexLen = 64
)

// isHexDigit reports whether b is one of [0-9a-fA-F].
func isHexDigit(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

// validateHashedPassword does a lightweight shape check on a stored-hash
// byte string for the given auth plugin. It does NOT verify cryptographic
// correctness — that happens during the actual handshake — it just rejects
// obvious format mismatches (empty, wrong length, missing structure) so
// callers find out at AddUser time, not on the first failed login.
func validateHashedPassword(authPluginName string, hash []byte) error {
	if len(hash) == 0 {
		return errors.Errorf("invalid hashed password for %s: empty", authPluginName)
	}
	switch authPluginName {
	case mysql.AUTH_NATIVE_PASSWORD:
		// Stored form is exactly SHA1(SHA1(plaintext)) = 20 bytes.
		if len(hash) != mysqlNativePasswordHashLen {
			return errors.Errorf("invalid hashed password length for %s: expected %d bytes, got %d", authPluginName, mysqlNativePasswordHashLen, len(hash))
		}
		return nil
	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		// Standard form is "$A$<iter-hex>$<salt><hash>" with the salt
		// (auth.SALT_LENGTH = 20 bytes) and hash (43 bytes) concatenated
		// in the final segment — see auth.NewHashPassword and
		// auth.CheckHashingPassword in pingcap/tidb. We require a 4-part
		// split with hashType "A", a non-empty hex iteration count, and
		// a final segment of exactly salt+hash bytes. The exact-length
		// check both prevents the upstream panic on parts[3][:SALT_LENGTH]
		// and ensures we don't store hashes the verifier would always
		// reject.
		parts := bytes.Split(hash, []byte("$"))
		if len(parts) != 4 || len(parts[0]) != 0 {
			return errors.Errorf("invalid hashed password for %s: expected $A$<iter>$<salt><hash> form", authPluginName)
		}
		if string(parts[1]) != "A" {
			return errors.Errorf("invalid hashed password for %s: expected hash type 'A', got %q", authPluginName, parts[1])
		}
		if len(parts[2]) == 0 {
			return errors.Errorf("invalid hashed password for %s: missing iteration count", authPluginName)
		}
		for _, b := range parts[2] {
			if !isHexDigit(b) {
				return errors.Errorf("invalid hashed password for %s: iteration count must be hex", authPluginName)
			}
		}
		if want, got := cachingSha2SaltLen+cachingSha2HashLen, len(parts[3]); got != want {
			return errors.Errorf("invalid hashed password for %s: final segment must be exactly %d bytes (salt+hash), got %d", authPluginName, want, got)
		}
		return nil
	case mysql.AUTH_SHA256_PASSWORD:
		// Standard form is "$<iter-decimal>$<salt:mysql.SALT_LENGTH=16>$<hashHex:64>"
		// — see mysql.NewSha256PasswordHash and mysql.Check256HashingPassword.
		// We require an exact-length salt and a 64-char hex digest because
		// that is what the verifier produces when reconstructing for the
		// equality check; any other shape is a stored-format mismatch and
		// would never authenticate, so it's better to reject up front.
		parts := bytes.Split(hash, []byte("$"))
		if len(parts) != 4 || len(parts[0]) != 0 {
			return errors.Errorf("invalid hashed password for %s: expected $<iter>$<salt>$<hashHex> form", authPluginName)
		}
		if len(parts[1]) == 0 {
			return errors.Errorf("invalid hashed password for %s: missing iteration count", authPluginName)
		}
		for _, b := range parts[1] {
			if b < '0' || b > '9' {
				return errors.Errorf("invalid hashed password for %s: iteration count must be decimal", authPluginName)
			}
		}
		if len(parts[2]) != mysql.SALT_LENGTH {
			return errors.Errorf("invalid hashed password for %s: salt must be exactly %d bytes (got %d)", authPluginName, mysql.SALT_LENGTH, len(parts[2]))
		}
		if len(parts[3]) != sha256HashHexLen {
			return errors.Errorf("invalid hashed password for %s: hash segment must be exactly %d hex chars (got %d)", authPluginName, sha256HashHexLen, len(parts[3]))
		}
		for _, b := range parts[3] {
			if !isHexDigit(b) {
				return errors.Errorf("invalid hashed password for %s: hash segment must be hex", authPluginName)
			}
		}
		return nil
	case mysql.AUTH_CLEAR_PASSWORD:
		// Clear-password is plaintext on the wire; there is no meaningful
		// "hashed" form. Callers should use AddUser instead.
		return errors.Errorf("AddUserWithHashedPassword does not apply to %s; use AddUser with the plaintext", authPluginName)
	default:
		return errors.Errorf("unknown authentication plugin name '%s'", authPluginName)
	}
}

func (h *InMemoryAuthenticationHandler) OnAuthSuccess(conn *Conn) error {
	return nil
}

func (h *InMemoryAuthenticationHandler) OnAuthFailure(conn *Conn, err error) {
}
