package server

import (
	"bytes"
	"testing"

	"github.com/salamin-tr-galt/go-mysql/mocks"
	"github.com/salamin-tr-galt/go-mysql/mysql"
	"github.com/stretchr/testify/mock"
)

func TestReadAuthData(t *testing.T) {
	c := &Conn{
		capability: mysql.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
	}

	data := []byte{141, 174, 255, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 114, 111, 111, 116, 0, 20, 190, 183, 72, 209, 170, 60, 191, 100, 227, 81, 203, 221, 190, 14, 213, 116, 244, 140, 90, 121, 109, 121, 115, 113, 108, 95, 112, 101, 114, 102, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}

	// test out of range index returns 'bad handshake' error
	_, _, _, err := c.readAuthData(data, len(data))
	if err == nil || err.Error() != "ERROR 1043 (08S01): Bad handshake" {
		t.Fatal("expected error, got nil")
	}

	// test good index position reads auth data
	_, _, readBytes, err := c.readAuthData(data, len(data)-1)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if readBytes != len(data)-1 {
		t.Fatalf("expected %d read bytes, got %d", len(data)-1, readBytes)
	}
}

func TestDecodeFirstPart(t *testing.T) {
	data := []byte{141, 174, 255, 1, 0, 0, 0, 1, 8}

	c := &Conn{}

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
	handler := &mocks.Handler{}
	c := &Conn{
		h: handler,
	}
	c.SetCapability(mysql.CLIENT_CONNECT_WITH_DB)
	var dbName string

	// when handler's UseDB is called, copy dbName to local variable
	handler.On("UseDB", mock.IsType("")).Return(nil).Once().RunFn = func(args mock.Arguments) {
		dbName = args[0].(string)
	}

	// example data from
	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41
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

	var err error
	pos, err = c.readDb(data, pos)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if pos != 66 { // 61 + len("test") + 1
		t.Fatalf("unexpected pos, got %d", pos)
	}

	if dbName != "test" {
		t.Fatalf("unexpected db, got %s", dbName)
	}

	handler.AssertExpectations(t)
}

func TestReadPluginName(t *testing.T) {
	// example data from
	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41
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

		pos = c.readPluginName(mysqlNativePassword, pos)
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

		pos = c.readPluginName(otherPlugin, pos)
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

		pos = c.readPluginName(mysqlNativePassword, pos)
		if pos != 123 { // capability not set, so same as initial pos
			t.Fatalf("unexpected pos, got %d", pos)
		}

		if c.authPluginName != mysql.AUTH_NATIVE_PASSWORD {
			t.Fatalf("unexpected plugin name, got %s", c.authPluginName)
		}
	})
}

func TestReadAttributes(t *testing.T) {
	var err error
	// example data from
	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41
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
