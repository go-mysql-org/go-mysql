package driver

import (
	"database/sql"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnector_OpenDB(t *testing.T) {
	srv := createMockServer(t)
	defer srv.Stop()

	connector := Connector{
		Addr:     "127.0.0.1:3307",
		User:     *testUser,
		Password: *testPassword,
		DB:       "test",
		Params: url.Values{
			// Just make sure Params are accepted when using Connector directly.
			"timeout": []string{"1s"},
		},
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	var a uint64
	var b string
	err := db.QueryRow("select * from table;").Scan(&a, &b)
	require.NoError(t, err)
	require.EqualValues(t, 1, a)
	require.Equal(t, "hello world", b)
}
