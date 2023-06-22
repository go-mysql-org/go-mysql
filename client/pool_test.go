package client

import (
	"context"
	"fmt"

	"github.com/go-mysql-org/go-mysql/test_util"
	. "github.com/pingcap/check"
	"github.com/siddontang/go-log/log"
)

type poolTestSuite struct {
	port string
}

func (s poolTestSuite) TestPool_Close(c *C) {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	pool := NewPool(log.Debugf, 5, 10, 5, addr, *testUser, *testPassword, "")
	conn, err := pool.GetConn(context.Background())
	c.Assert(err, IsNil)
	err = conn.Ping()
	c.Assert(err, IsNil)
	pool.PutConn(conn)
	pool.Close()
	var poolStats ConnectionStats
	pool.GetStats(&poolStats)
	c.Assert(poolStats.TotalCount, Equals, 0)
	c.Assert(pool.readyConnection, HasLen, 0)
	_, err = pool.GetConn(context.Background())
	c.Assert(err, NotNil)
}
