package client

import (
	"bytes"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestConnGenAttributes(t *testing.T) {
	c := &Conn{
		// example data from
		// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41
		attributes: map[string]string{
			"_os":             "debian6.0",
			"_client_name":    "libmysql",
			"_pid":            "22344",
			"_client_version": "5.6.6-m9",
			"_platform":       "x86_64",
			"foo":             "bar",
		},
	}

	data := c.genAttributes()

	// the order of the attributes map cannot be guaranteed so to test the content
	// of the attribute data we need to check its partial contents

	if len(data) != 98 {
		t.Fatalf("unexpected data length, got %d", len(data))
	}
	if data[0] != 0x61 {
		t.Fatalf("unexpected length-encoded int, got %#x", data[0])
	}

	for k, v := range c.attributes {
		fixt := append(mysql.PutLengthEncodedString([]byte(k)), mysql.PutLengthEncodedString([]byte(v))...)
		if !bytes.Contains(data, fixt) {
			t.Fatalf("%s attribute not found", k)
		}
	}
}
