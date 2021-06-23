package server

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/check"
)

type connTestSuite struct {
}

var _ = check.Suite(&connTestSuite{})

func (t *connTestSuite) TestStatus(c *check.C) {
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
		c.Assert(conn.HasStatus(f), check.IsFalse)
		conn.SetStatus(f)
		c.Assert(conn.HasStatus(f), check.IsTrue)
		conn.UnsetStatus(f)
		c.Assert(conn.HasStatus(f), check.IsFalse)
	}

	// check special flag setters
	// IsAutoCommit
	c.Assert(conn.IsAutoCommit(), check.IsFalse)
	conn.SetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)
	c.Assert(conn.IsAutoCommit(), check.IsTrue)
	conn.UnsetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)

	// IsInTransaction
	c.Assert(conn.IsInTransaction(), check.IsFalse)
	conn.SetStatus(mysql.SERVER_STATUS_IN_TRANS)
	c.Assert(conn.IsInTransaction(), check.IsTrue)
	conn.UnsetStatus(mysql.SERVER_STATUS_IN_TRANS)

	// SetInTransaction & ClearInTransaction
	c.Assert(conn.HasStatus(mysql.SERVER_STATUS_IN_TRANS), check.IsFalse)
	conn.SetInTransaction()
	c.Assert(conn.HasStatus(mysql.SERVER_STATUS_IN_TRANS), check.IsTrue)
	conn.ClearInTransaction()
	c.Assert(conn.HasStatus(mysql.SERVER_STATUS_IN_TRANS), check.IsFalse)
}

func (t *connTestSuite) TestCapability(c *check.C) {
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

	for _, cap := range caps {
		c.Assert(conn.HasCapability(cap), check.IsFalse)
		conn.SetCapability(cap)
		c.Assert(conn.HasCapability(cap), check.IsTrue)
		c.Assert(conn.Capability()&cap > 0, check.IsTrue)
		conn.UnsetCapability(cap)
		c.Assert(conn.HasCapability(cap), check.IsFalse)
	}
}
