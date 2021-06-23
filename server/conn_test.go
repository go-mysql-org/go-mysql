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
