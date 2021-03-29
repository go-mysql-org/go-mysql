package server

import (
	"testing"

	"github.com/siddontang/go-mysql/mysql"
)

func TestReadAuthData(t *testing.T) {
	c := &Conn{
		capability: mysql.CLIENT_SECURE_CONNECTION,
	}

	data := []byte{141, 174, 255, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 114, 111, 111, 116, 0, 20, 28, 152, 53, 72, 150, 120, 94, 151, 3, 104, 218, 30, 186, 82, 221, 123, 12, 50, 148, 88, 109, 121, 115, 113, 108, 95, 112, 101, 114, 102, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0}

	// test out of range index returns 'bad handshake' error
	_, _, _, err := c.readAuthData(data, len(data))
	if err == nil || err.Error() != "ERROR 1043 (08S01): Bad handshake" {
		t.Fatal("expected error, got nil")
	}

	// test good index position reads auth data
	if _, _, _, err := c.readAuthData(data, len(data)-1); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
