package server

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestStatus(t *testing.T) {
	conn := Conn{}

	flags := []uint16{
		mysql.SERVER_STATUS_IN_TRANS,
		mysql.SERVER_STATUS_AUTOCOMMIT,
		mysql.SERVER_MORE_RESULTS_EXISTS,
		mysql.SERVER_STATUS_NO_GOOD_INDEX_USED,
		mysql.SERVER_STATUS_NO_INDEX_USED,
		mysql.SERVER_STATUS_CURSOR_EXISTS,
		mysql.SERVER_STATUS_LAST_ROW_SEND,
		mysql.SERVER_STATUS_DB_DROPPED,
		mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED,
		mysql.SERVER_STATUS_METADATA_CHANGED,
		mysql.SERVER_QUERY_WAS_SLOW,
		mysql.SERVER_PS_OUT_PARAMS,
	}

	for _, f := range flags {
		require.False(t, conn.HasStatus(f))
		conn.SetStatus(f)
		require.True(t, conn.HasStatus(f))
		conn.UnsetStatus(f)
		require.False(t, conn.HasStatus(f))
	}

	// check special flag setters
	// IsAutoCommit
	require.False(t, conn.IsAutoCommit())
	conn.SetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)
	require.True(t, conn.IsAutoCommit())
	conn.UnsetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)

	// IsInTransaction
	require.False(t, conn.IsInTransaction())
	conn.SetStatus(mysql.SERVER_STATUS_IN_TRANS)
	require.True(t, conn.IsInTransaction())
	conn.UnsetStatus(mysql.SERVER_STATUS_IN_TRANS)

	// SetInTransaction & ClearInTransaction
	require.False(t, conn.HasStatus(mysql.SERVER_STATUS_IN_TRANS))
	conn.SetInTransaction()
	require.True(t, conn.HasStatus(mysql.SERVER_STATUS_IN_TRANS))
	conn.ClearInTransaction()
	require.False(t, conn.HasStatus(mysql.SERVER_STATUS_IN_TRANS))
}

func TestCapability(t *testing.T) {
	conn := Conn{}

	caps := []uint32{
		mysql.CLIENT_LONG_PASSWORD,
		mysql.CLIENT_FOUND_ROWS,
		mysql.CLIENT_LONG_FLAG,
		mysql.CLIENT_CONNECT_WITH_DB,
		mysql.CLIENT_NO_SCHEMA,
		mysql.CLIENT_COMPRESS,
		mysql.CLIENT_ODBC,
		mysql.CLIENT_LOCAL_FILES,
		mysql.CLIENT_IGNORE_SPACE,
		mysql.CLIENT_PROTOCOL_41,
		mysql.CLIENT_INTERACTIVE,
		mysql.CLIENT_SSL,
		mysql.CLIENT_IGNORE_SIGPIPE,
		mysql.CLIENT_TRANSACTIONS,
		mysql.CLIENT_RESERVED,
		mysql.CLIENT_SECURE_CONNECTION,
		mysql.CLIENT_MULTI_STATEMENTS,
		mysql.CLIENT_MULTI_RESULTS,
		mysql.CLIENT_PS_MULTI_RESULTS,
		mysql.CLIENT_PLUGIN_AUTH,
		mysql.CLIENT_CONNECT_ATTRS,
		mysql.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
	}

	for _, capI := range caps {
		require.False(t, conn.HasCapability(capI))
		conn.SetCapability(capI)
		require.True(t, conn.HasCapability(capI))
		require.True(t, conn.Capability()&capI > 0)
		conn.UnsetCapability(capI)
		require.False(t, conn.HasCapability(capI))
	}
}
