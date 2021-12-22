package server

import (
	"bytes"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
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
