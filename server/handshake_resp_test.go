package server

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mocks"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/go-mysql-org/go-mysql/test_util/test_keys"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestReadAuthData(t *testing.T) {
	c := &Conn{
		capability: mysql.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
	}

	data := []byte{141, 174, 255, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 114, 111, 111, 116, 0, 20, 190, 183, 72, 209, 170, 60, 191, 100, 227, 81, 203, 221, 190, 14, 213, 116, 244, 140, 90, 121, 109, 121, 115, 113, 108, 95, 112, 101, 114, 102, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}

	// test out of range index returns 'bad handshake' error
	_, _, _, err := c.readAuthData(data, len(data))
	require.ErrorContains(t, err, "ERROR 1043 (08S01): Bad handshake")

	// test good index position reads auth data
	_, _, readBytes, err := c.readAuthData(data, len(data)-1)
	require.NoError(t, err)
	require.Equal(t, len(data)-1, readBytes)
}

func TestReadAuthData_LengthPastEnd(t *testing.T) {
	c := &Conn{capability: mysql.CLIENT_SECURE_CONNECTION}

	// Auth data that ends at the end of the packet is read.
	auth, authLen, pos, err := c.readAuthData(append(make([]byte, 0, 8), 0x01, 0xab), 0)
	require.NoError(t, err)
	require.Equal(t, []byte{0xab}, auth)
	require.Equal(t, 1, authLen)
	require.Equal(t, 1, pos)

	// The auth data length is 1, but the packet ends after it. The slice has
	// spare capacity, as packets from the packet layer usually do.
	_, _, _, err = c.readAuthData(append(make([]byte, 0, 8), 0x01), 0)
	var myErr *mysql.MyError
	require.ErrorAs(t, err, &myErr)
	require.EqualValues(t, mysql.ER_HANDSHAKE_ERROR, myErr.Code)
}

func TestDecodeFirstPart(t *testing.T) {
	c := &Conn{}

	// test out of range index returns 'bad handshake' error
	_, _, err := c.decodeFirstPart([]byte{141, 174})
	if err == nil || err.Error() != "ERROR 1043 (08S01): Bad handshake" {
		t.Fatal("expected error, got nil")
	}

	// test good index position
	data := []byte{141, 174, 255, 1, 0, 0, 0, 1, 8}

	result, pos, err := c.decodeFirstPart(data)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Fatal("expected same data, got something else")
	}
	if pos != 32 {
		t.Fatalf("unexpected pos, got %d", pos)
	}
	if c.capability != 33533581 {
		t.Fatalf("unexpected capability, got %d", c.capability)
	}
	if c.charset != 8 {
		t.Fatalf("unexpected capability, got %d", c.capability)
	}
}

func TestReadDB(t *testing.T) {
	c := &Conn{}
	c.SetCapability(mysql.CLIENT_CONNECT_WITH_DB)

	// example data from
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41
	data := []byte{
		0x54, 0x00, 0x00, 0x01, 0x8d, 0xa6, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x70, 0x61, 0x6d, 0x00, 0x14, 0xab, 0x09, 0xee, 0xf6, 0xbc, 0xb1, 0x32,
		0x3e, 0x61, 0x14, 0x38, 0x65, 0xc0, 0x99, 0x1d, 0x95, 0x7d, 0x75, 0xd4,
		0x47, 0x74, 0x65, 0x73, 0x74, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f,
		0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77,
		0x6f, 0x72, 0x64, 0x00,
	}
	pos := 61

	dbName, pos, err := c.readDb(data, pos)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if pos != 66 { // 61 + len("test") + 1
		t.Fatalf("unexpected pos, got %d", pos)
	}

	if dbName != "test" {
		t.Fatalf("unexpected db, got %s", dbName)
	}
}

func TestUseDBAfterAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name     string
		srv      *Server
		userAuth string // the auth method the user's password is stored for
	}{
		{
			name:     "no auth switch",
			srv:      NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil),
			userAuth: mysql.AUTH_NATIVE_PASSWORD,
		},
		{
			// The client starts with sha256_password, and the server switches
			// it to the user's mysql_native_password.
			name:     "auth switch",
			srv:      NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, test_keys.RSAKey(), nil),
			userAuth: mysql.AUTH_NATIVE_PASSWORD,
		},
		{
			// Nothing is cached for the first logins, so the server asks for
			// the full password, which the client encrypts with its RSA key.
			name:     "caching_sha2_password full authentication",
			srv:      NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, test_keys.RSAKey(), nil),
			userAuth: mysql.AUTH_CACHING_SHA2_PASSWORD,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.srv
			authHandler := NewInMemoryAuthenticationHandler()
			require.NoError(t, authHandler.AddUser("root", "secret", tc.userAuth))

			l, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer l.Close()

			handler := &mocks.Handler{}
			handler.On("UseDB", "test").Return(nil)
			handler.On("UseDB", "missing").Return(mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, "missing"))

			go func() {
				for {
					conn, err := l.Accept()
					if err != nil {
						return
					}
					if c, err := srv.NewCustomizedConn(conn, authHandler, handler); err == nil {
						c.Close()
					}
				}
			}()
			addr := l.Addr().String()

			// A client that fails to authenticate must not reach the handler.
			_, err = client.Connect(addr, "nobody", "secret", "test")
			require.Error(t, err)
			_, err = client.Connect(addr, "root", "wrong", "test")
			require.Error(t, err)
			handler.AssertNotCalled(t, "UseDB", mock.Anything)

			conn, err := client.Connect(addr, "root", "secret", "test")
			require.NoError(t, err)
			require.NoError(t, conn.Close())
			handler.AssertNumberOfCalls(t, "UseDB", 1)

			// An error from UseDB reaches the client.
			_, err = client.Connect(addr, "root", "secret", "missing")
			require.ErrorContains(t, err, "Unknown database 'missing'")
		})
	}
}

// As in MySQL, an empty database name in the handshake response selects no
// database. The client package never sends one, so this writes the handshake
// response itself.
func TestUseDBSkipsEmptyName(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	srv := NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	authHandler := NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, authHandler.AddUser("root", "secret"))
	handler := &mocks.Handler{}
	handler.On("UseDB", mock.Anything).Return(nil)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		if c, err := srv.NewCustomizedConn(conn, authHandler, handler); err == nil {
			c.Close()
		}
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	pc := packet.NewConn(conn)

	greeting, err := pc.ReadPacket()
	require.NoError(t, err)
	// The scramble's first 8 bytes follow the protocol version, the server
	// version and the connection id. The other 12 follow a filler byte, the
	// capability flags, the charset, the status, the scramble length and 10
	// reserved bytes.
	pos := bytes.IndexByte(greeting[1:], 0x00) + 2 + 4
	salt := append([]byte{}, greeting[pos:pos+8]...)
	pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10
	salt = append(salt, greeting[pos:pos+12]...)

	auth := mysql.CalcNativePassword(salt, []byte("secret"))
	data := make([]byte, 4) // the packet header, which WritePacket fills in
	data = binary.LittleEndian.AppendUint32(data, mysql.CLIENT_PROTOCOL_41|mysql.CLIENT_SECURE_CONNECTION|
		mysql.CLIENT_PLUGIN_AUTH|mysql.CLIENT_CONNECT_WITH_DB)
	data = binary.LittleEndian.AppendUint32(data, 0) // max packet size
	data = append(data, mysql.DEFAULT_COLLATION_ID)
	data = append(data, make([]byte, 23)...)
	data = append(data, "root\x00"...)
	data = append(data, byte(len(auth)))
	data = append(data, auth...)
	data = append(data, 0x00) // the empty database name
	data = append(data, mysql.AUTH_NATIVE_PASSWORD+"\x00"...)
	require.NoError(t, pc.WritePacket(data))

	resp, err := pc.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, mysql.OK_HEADER, resp[0])
	handler.AssertNotCalled(t, "UseDB", mock.Anything)
}

func TestReadPluginName(t *testing.T) {
	// example data from
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41
	mysqlNativePassword := []byte{
		0x54, 0x00, 0x00, 0x01, 0x8d, 0xa6, 0x0f, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x70, 0x61, 0x6d, 0x00, 0x14, 0xab, 0x09, 0xee,
		0xf6, 0xbc, 0xb1, 0x32, 0x3e, 0x61, 0x14, 0x38, 0x65, 0xc0, 0x99,
		0x1d, 0x95, 0x7d, 0x75, 0xd4, 0x47, 0x74, 0x65, 0x73, 0x74, 0x00,
		0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76,
		0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00,
	}

	// altered example data so it has different auth plugin
	otherPlugin := []byte{
		0x54, 0x00, 0x00, 0x01, 0x8d, 0xa6, 0x0f, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x70, 0x61, 0x6d, 0x00, 0x14, 0xab, 0x09, 0xee,
		0xf6, 0xbc, 0xb1, 0x32, 0x3e, 0x61, 0x14, 0x38, 0x65, 0xc0, 0x99,
		0x1d, 0x95, 0x7d, 0x75, 0xd4, 0x47, 0x74, 0x65, 0x73, 0x74, 0x00,
		0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72, 0x00,
	}

	t.Run("mysql_native_password from plugin name", func(t *testing.T) {
		c := &Conn{}
		c.SetCapability(mysql.CLIENT_PLUGIN_AUTH)
		pos := 66

		pos, _ = c.readPluginName(mysqlNativePassword, pos)
		if pos != 88 { // 66 + len("mysql_native_password") + 1
			t.Fatalf("unexpected pos, got %d", pos)
		}

		if c.authPluginName != "mysql_native_password" {
			t.Fatalf("unexpected plugin name, got %s", c.authPluginName)
		}
	})

	t.Run("other plugin", func(t *testing.T) {
		c := &Conn{}
		c.SetCapability(mysql.CLIENT_PLUGIN_AUTH)
		pos := 66

		pos, _ = c.readPluginName(otherPlugin, pos)
		if pos != 73 { // 66 + len("foobar") + 1
			t.Fatalf("unexpected pos, got %d", pos)
		}

		if c.authPluginName != "foobar" {
			t.Fatalf("unexpected plugin name, got %s", c.authPluginName)
		}
	})

	t.Run("mysql_native_password as default", func(t *testing.T) {
		c := &Conn{}
		pos := 123 // can be anything

		pos, _ = c.readPluginName(mysqlNativePassword, pos)
		if pos != 123 { // capability not set, so same as initial pos
			t.Fatalf("unexpected pos, got %d", pos)
		}

		if c.authPluginName != mysql.AUTH_NATIVE_PASSWORD {
			t.Fatalf("unexpected plugin name, got %s", c.authPluginName)
		}
	})

	t.Run("plugin auth set but name not NUL-terminated", func(t *testing.T) {
		c := &Conn{capability: mysql.CLIENT_PLUGIN_AUTH}

		_, err := c.readPluginName([]byte("mysql_native_password"), 0) // no 0x00
		if err == nil {
			t.Fatal("expected error for non-terminated plugin name, got nil")
		}
	})

	t.Run("plugin auth set with empty buffer", func(t *testing.T) {
		c := &Conn{capability: mysql.CLIENT_PLUGIN_AUTH}

		_, err := c.readPluginName([]byte{}, 0)
		if err == nil {
			t.Fatal("expected error for empty buffer, got nil")
		}
	})
}

func TestReadUserName_MissingNulTerminator(t *testing.T) {
	c := &Conn{}
	if _, err := c.readUserName([]byte("someuser"), 0); err == nil {
		t.Fatal("expected error for non-terminated username, got nil")
	}
}

func TestReadDb_MissingNulTerminator(t *testing.T) {
	c := &Conn{capability: mysql.CLIENT_CONNECT_WITH_DB}
	// CLIENT_CONNECT_WITH_DB set, non-empty db field, but no NUL.
	if _, _, err := c.readDb([]byte("somedb"), 0); err == nil {
		t.Fatal("expected error for non-terminated db name, got nil")
	}
}

func TestReadAttributes(t *testing.T) {
	var err error
	// example data from
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41
	data := []byte{
		0xb2, 0x00, 0x00, 0x01, 0x85, 0xa2, 0x1e, 0x00, 0x00, 0x00,
		0x00, 0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x14, 0x22, 0x50, 0x79, 0xa2,
		0x12, 0xd4, 0xe8, 0x82, 0xe5, 0xb3, 0xf4, 0x1a, 0x97, 0x75, 0x6b, 0xc8,
		0xbe, 0xdb, 0x9f, 0x80, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61,
		0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72,
		0x64, 0x00, 0x61, 0x03, 0x5f, 0x6f, 0x73, 0x09, 0x64, 0x65, 0x62, 0x69,
		0x61, 0x6e, 0x36, 0x2e, 0x30, 0x0c, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e,
		0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x08, 0x6c, 0x69, 0x62, 0x6d, 0x79,
		0x73, 0x71, 0x6c, 0x04, 0x5f, 0x70, 0x69, 0x64, 0x05, 0x32, 0x32, 0x33,
		0x34, 0x34, 0x0f, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x76,
		0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x08, 0x35, 0x2e, 0x36, 0x2e, 0x36,
		0x2d, 0x6d, 0x39, 0x09, 0x5f, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72,
		0x6d, 0x06, 0x78, 0x38, 0x36, 0x5f, 0x36, 0x34, 0x03, 0x66, 0x6f, 0x6f,
		0x03, 0x62, 0x61, 0x72,
	}
	pos := 84

	c := &Conn{}

	pos, err = c.readAttributes(data, pos)
	if err != nil {
		t.Fatalf("unexpected error: got %v", err)
	}

	if pos != 182 {
		t.Fatalf("unexpected position: got %d", pos)
	}

	if len(c.attributes) != 6 {
		t.Fatalf("unexpected attribute length: got %d", len(c.attributes))
	}

	fixture := map[string]string{
		"_client_name":    "libmysql",
		"_client_version": "5.6.6-m9",
		"_os":             "debian6.0",
		"_pid":            "22344",
		"_platform":       "x86_64",
		"foo":             "bar",
	}

	for k, v := range fixture {
		if vv := c.attributes[k]; vv != v {
			t.Fatalf("unexpected value for %s, got %s instead of %s", k, vv, v)
		}
	}
}

func TestReadAttributes_TruncatedLength(t *testing.T) {
	for _, data := range [][]byte{
		{0xfc, 0x01},                   // the length of all attributes
		{0x02, 0xfc, 0x01},             // the length of the first key
		{0x04, 0x01, 0x6b, 0xfc, 0x01}, // the length of the first value
	} {
		c := &Conn{}
		_, err := c.readAttributes(data, 0)
		var myErr *mysql.MyError
		require.ErrorAs(t, err, &myErr, "%x", data)
		require.EqualValues(t, mysql.ER_HANDSHAKE_ERROR, myErr.Code, "%x", data)
	}
}
