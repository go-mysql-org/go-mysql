package client

import (
	"net"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
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

	require.Len(t, data, 98)
	require.Equal(t, byte(0x61), data[0])

	for k, v := range c.attributes {
		fixt := append(mysql.PutLengthEncodedString([]byte(k)), mysql.PutLengthEncodedString([]byte(v))...)
		require.Subset(t, data, fixt)
	}
}

func TestConnCollation(t *testing.T) {
	collations := []string{
		"big5_chinese_ci",
		"utf8_general_ci",
		"utf8mb4_0900_ai_ci",
		"utf8mb4_de_pb_0900_ai_ci",
		"utf8mb4_ja_0900_as_cs",
		"utf8mb4_0900_bin",
		"utf8mb4_zh_pinyin_tidb_as_cs",
	}

	// test all supported collations by calling writeAuthHandshake() and reading the bytes
	// sent to the server to ensure the collation id is set correctly
	for _, c := range collations {
		collation, err := charset.GetCollationByName(c)
		require.NoError(t, err)
		server := sendAuthResponse(t, collation.Name)
		// read the all the bytes of the handshake response so that client goroutine can complete without blocking
		// on the server read.
		handShakeResponse := make([]byte, 128)
		_, err = server.Read(handShakeResponse)
		require.NoError(t, err)

		// validate the collation id is set correctly
		// if the collation ID is <= 255 the collation ID is stored in the 12th byte
		if collation.ID <= 255 {
			require.Equal(t, byte(collation.ID), handShakeResponse[12])
		} else {
			// if the collation ID is > 255 the collation ID should just be the lower-8 bits
			require.Equal(t, byte(collation.ID&0xff), handShakeResponse[12])
		}

		// the 13th byte should always be 0x00
		require.Equal(t, byte(0x00), handShakeResponse[13])

		// sanity check: validate the 22 bytes of filler with value 0x00 are set correctly
		for i := 13; i < 13+23; i++ {
			require.Equal(t, byte(0x00), handShakeResponse[i])
		}

		// and finally the username
		username := string(handShakeResponse[36:40])
		require.Equal(t, "test", username)

		require.NoError(t, server.Close())
	}
}

func sendAuthResponse(t *testing.T, collation string) net.Conn {
	server, client := net.Pipe()
	c := &Conn{
		Conn: &packet.Conn{
			Conn: client,
		},
		authPluginName: "mysql_native_password",
		user:           "test",
		db:             "test",
		password:       "test",
		proto:          "tcp",
		collation:      collation,
		salt:           ([]byte)("123456781234567812345678"),
	}

	go func() {
		err := c.writeAuthHandshake()
		require.NoError(t, err)
		err = c.Close()
		require.NoError(t, err)
	}()
	return server
}
