package server

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// changeUserHandler records COM_CHANGE_USER and statement lifecycle calls.
type changeUserHandler struct {
	EmptyHandler
	mu          sync.Mutex
	changes     []string
	closedStmts int
	changeErr   error
}

func (h *changeUserHandler) HandleChangeUser(user string, dbName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.changes = append(h.changes, user+"/"+dbName)
	return h.changeErr
}

func (h *changeUserHandler) HandleQuery(string) (*mysql.Result, error) {
	return &mysql.Result{}, nil
}

func (h *changeUserHandler) HandleStmtPrepare(string) (int, int, any, error) {
	return 0, 0, nil, nil
}

func (h *changeUserHandler) HandleStmtExecute(any, string, []any) (*mysql.Result, error) {
	return &mysql.Result{}, nil
}

func (h *changeUserHandler) HandleStmtClose(any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closedStmts++
	return nil
}

func (h *changeUserHandler) state() ([]string, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.changes...), h.closedStmts
}

// serveChangeUser serves one connection with handler h and returns a client
// connected as user u1.
func serveChangeUser(t *testing.T, auth AuthenticationHandler, h Handler) *client.Conn {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		co, err := NewDefaultServer().NewCustomizedConn(conn, auth, h)
		if err != nil {
			return
		}
		//nolint:revive // loop drains commands; work is in the condition
		for co.HandleCommand() == nil {
		}
	}()
	c, err := client.Connect(l.Addr().String(), "u1", "p1", "db1")
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func newChangeUserAuth(t *testing.T) *hookTrackingAuthenticationHandler {
	t.Helper()
	auth := &hookTrackingAuthenticationHandler{
		InMemoryAuthenticationHandler: NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD),
	}
	require.NoError(t, auth.AddUser("u1", "p1"))
	require.NoError(t, auth.AddUser("u2", "p2", mysql.AUTH_CACHING_SHA2_PASSWORD))
	require.NoError(t, auth.AddUser("u3", "p3", mysql.AUTH_SHA256_PASSWORD))
	return auth
}

func TestChangeUser(t *testing.T) {
	auth := newChangeUserAuth(t)
	h := &changeUserHandler{}
	c := serveChangeUser(t, auth, h)

	stmt, err := c.Prepare("SELECT 1")
	require.NoError(t, err)

	// u2 authenticates with caching_sha2_password: an auth switch from the
	// native session, then full authentication (nothing is cached yet).
	require.NoError(t, c.ChangeUser("u2", "p2", "db2"))
	changes, closed := h.state()
	require.Equal(t, []string{"u2/db2"}, changes)
	require.Equal(t, 1, closed, "prepared statements are deallocated")
	_, err = stmt.Execute()
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.Equal(t, uint16(mysql.ER_UNKNOWN_STMT_HANDLER), myErr.Code)

	// Again as u2: the cache now holds u2, so this is the fast path.
	require.NoError(t, c.ChangeUser("u2", "p2", ""))
	// u3 (sha256_password, RSA-encrypted over plain TCP), then back to native.
	require.NoError(t, c.ChangeUser("u3", "p3", "db3"))
	require.NoError(t, c.ChangeUser("u1", "p1", "db1"))

	changes, _ = h.state()
	require.Equal(t, []string{"u2/db2", "u2/", "u3/db3", "u1/db1"}, changes)
	require.Equal(t, int32(5), auth.onSuccessCalled.Load(), "the handshake and every change")
	require.Equal(t, int32(0), auth.onFailureCalled.Load())

	_, err = c.Execute("SELECT 1")
	require.NoError(t, err, "the connection is still usable")
}

func TestChangeUserAuthFailureClosesConnection(t *testing.T) {
	for name, tc := range map[string]struct{ user, password string }{
		"wrong password":               {"u1", "nope"},
		"empty password":               {"u1", ""},
		"unknown user":                 {"nobody", "x"},
		"unknown user, empty password": {"nobody", ""},
	} {
		t.Run(name, func(t *testing.T) {
			auth := newChangeUserAuth(t)
			h := &changeUserHandler{}
			c := serveChangeUser(t, auth, h)

			err := c.ChangeUser(tc.user, tc.password, "db1")
			var myErr *mysql.MyError
			require.ErrorAs(t, err, &myErr)
			// As MySQL: every case, an unknown user included, is 1045, and
			// the message reports whether a password was sent.
			require.Equal(t, uint16(mysql.ER_ACCESS_DENIED_ERROR), myErr.Code)
			usingPassword := "YES"
			if tc.password == "" {
				usingPassword = "NO"
			}
			require.Contains(t, myErr.Message, "'"+tc.user+"'@")
			require.Contains(t, myErr.Message, "(using password: "+usingPassword+")")
			changes, _ := h.state()
			require.Empty(t, changes, "the handler is not called")
			require.Equal(t, int32(1), auth.onFailureCalled.Load())

			_, err = c.Execute("SELECT 1")
			require.Error(t, err, "MySQL closes the connection after a failed change")
		})
	}
}

func TestChangeUserAccessDeniedCode(t *testing.T) {
	c := serveChangeUser(t, newChangeUserAuth(t), &changeUserHandler{})
	err := c.ChangeUser("u1", "nope", "db1")
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.Equal(t, uint16(mysql.ER_ACCESS_DENIED_ERROR), myErr.Code)
	require.Contains(t, myErr.Message, "(using password: YES)")
}

func TestChangeUserHandlerErrorClosesConnection(t *testing.T) {
	h := &changeUserHandler{changeErr: mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, "nodb")}
	c := serveChangeUser(t, newChangeUserAuth(t), h)

	err := c.ChangeUser("u1", "p1", "nodb")
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.Equal(t, uint16(mysql.ER_BAD_DB_ERROR), myErr.Code)
	_, err = c.Execute("SELECT 1")
	require.Error(t, err)
}

func TestChangeUserWithoutHandlerSupport(t *testing.T) {
	// A handler without ChangeUserHandler gets the command as before, and
	// the connection stays open.
	c := serveChangeUser(t, newChangeUserAuth(t), &EmptyHandler{})
	err := c.ChangeUser("u1", "p1", "db1")
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.Equal(t, uint16(mysql.ER_UNKNOWN_ERROR), myErr.Code)
	require.NoError(t, c.Ping())
}

func TestParseChangeUser(t *testing.T) {
	c := &Conn{capability: mysql.CLIENT_PROTOCOL_41 | mysql.CLIENT_SECURE_CONNECTION |
		mysql.CLIENT_PLUGIN_AUTH | mysql.CLIENT_CONNECT_ATTRS}

	full := []byte("u\x00\x02ab" + "db\x00" + "\xff\x00" + "caching_sha2_password\x00")
	full = append(full, 0x06, 0x01, 'k', 0x03, 'v', 'a', 'l')
	req, err := c.parseChangeUser(full)
	require.NoError(t, err)
	require.Equal(t, &changeUserRequest{
		user: "u", auth: []byte("ab"), db: "db", charset: 255,
		plugin: mysql.AUTH_CACHING_SHA2_PASSWORD, attributes: map[string]string{"k": "val"},
	}, req)

	// Everything after the database is optional; no plugin name means
	// mysql_native_password.
	req, err = c.parseChangeUser([]byte("u\x00\x00\x00"))
	require.NoError(t, err)
	require.Equal(t, &changeUserRequest{user: "u", plugin: mysql.AUTH_NATIVE_PASSWORD}, req)

	for name, data := range map[string][]byte{
		"no user terminator":   []byte("u"),
		"auth past the end":    []byte("u\x00\x05ab"),
		"no db terminator":     []byte("u\x00\x00db"),
		"one-byte charset":     []byte("u\x00\x00\x00\xff"),
		"no plugin terminator": []byte("u\x00\x00\x00\xff\x00plugin"),
		"attributes past end":  append([]byte("u\x00\x00\x00\xff\x00p\x00"), 0x09, 0x01, 'k'),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := c.parseChangeUser(data)
			var myErr *mysql.MyError
			require.ErrorAs(t, err, &myErr)
			require.Equal(t, uint16(mysql.ER_MALFORMED_PACKET), myErr.Code)
		})
	}
}

func TestHandshakeUnknownUserIsAccessDenied(t *testing.T) {
	for password, usingPassword := range map[string]string{"x": "YES", "": "NO"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		done := make(chan struct{})
		go func() {
			defer close(done)
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_, _ = NewDefaultServer().NewCustomizedConn(conn, newChangeUserAuth(t), &EmptyHandler{})
		}()
		_, err = client.Connect(l.Addr().String(), "nobody", password, "")
		<-done
		l.Close()
		var myErr *mysql.MyError
		require.ErrorAs(t, err, &myErr)
		require.Equal(t, uint16(mysql.ER_ACCESS_DENIED_ERROR), myErr.Code)
		require.Contains(t, myErr.Message, "(using password: "+usingPassword+")")
	}
}

// metadataAuthHandler records what OnAuthSuccess sees of the connection.
type metadataAuthHandler struct {
	*InMemoryAuthenticationHandler
	mu   sync.Mutex
	seen []string
}

func (h *metadataAuthHandler) OnAuthSuccess(c *Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, fmt.Sprintf("%s charset=%d collation=%d attr=%s",
		c.GetUser(), c.Charset(), c.CollationID(), c.Attributes()["k"]))
	return nil
}

func TestChangeUserMetadataBeforeAuthHooks(t *testing.T) {
	auth := &metadataAuthHandler{InMemoryAuthenticationHandler: NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)}
	require.NoError(t, auth.AddUser("u1", "p1"))
	require.NoError(t, auth.AddUser("u2", "p2"))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	done := make(chan struct{})
	defer func() { <-done }()
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		co, err := NewDefaultServer().NewCustomizedConn(conn, auth, &changeUserHandler{})
		if err != nil {
			return
		}
		//nolint:revive // loop drains commands; work is in the condition
		for co.HandleCommand() == nil {
		}
	}()

	// utf8mb4_0900_as_cs is collation 278: the handshake carries only its
	// low byte (22), COM_CHANGE_USER all of it.
	c, err := client.Connect(l.Addr().String(), "u1", "p1", "", func(c *client.Conn) error {
		c.SetAttributes(map[string]string{"k": "v1"})
		return c.SetCollation("utf8mb4_0900_as_cs")
	})
	require.NoError(t, err)
	defer c.Close()
	c.SetAttributes(map[string]string{"k": "v2"})
	require.NoError(t, c.ChangeUser("u2", "p2", ""))

	auth.mu.Lock()
	defer auth.mu.Unlock()
	require.Equal(t, []string{
		"u1 charset=22 collation=22 attr=v1",
		"u2 charset=22 collation=278 attr=v2",
	}, auth.seen)
}

// Without CLIENT_PLUGIN_AUTH the request carries no plugin name, so the bytes
// after the collation are the connection attributes.
func TestParseChangeUserWithoutPluginAuth(t *testing.T) {
	c := &Conn{capability: mysql.CLIENT_PROTOCOL_41 | mysql.CLIENT_SECURE_CONNECTION |
		mysql.CLIENT_CONNECT_ATTRS}
	data := []byte("u\x00\x02ab" + "db\x00" + "\xff\x00")
	data = append(data, 0x06, 0x01, 'k', 0x03, 'v', 'a', 'l')
	req, err := c.parseChangeUser(data)
	require.NoError(t, err)
	require.Equal(t, &changeUserRequest{
		user: "u", auth: []byte("ab"), db: "db", charset: 255,
		plugin: mysql.AUTH_NATIVE_PASSWORD, attributes: map[string]string{"k": "val"},
	}, req)
}
