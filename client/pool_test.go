package client

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/test_util"
	"github.com/siddontang/go-log/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type poolTestSuite struct {
	suite.Suite
	port string
}

func TestPoolSuite(t *testing.T) {
	segs := strings.Split(*test_util.MysqlPort, ",")
	for _, seg := range segs {
		suite.Run(t, &poolTestSuite{port: seg})
	}
}

func (s *poolTestSuite) TestPool_Close() {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	pool, err := NewPoolWithOptions(addr, *testUser, *testPassword, "",
		WithPoolLimits(5, 10, 5),
		WithLogFunc(log.Debugf),
	)
	require.NoError(s.T(), err)

	conn, err := pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	err = conn.Ping()
	require.NoError(s.T(), err)
	pool.PutConn(conn)
	pool.Close()
	var poolStats ConnectionStats
	pool.GetStats(&poolStats)
	require.Equal(s.T(), 0, poolStats.IdleCount)
	require.Len(s.T(), pool.readyConnection, 0)
	_, err = pool.GetConn(context.Background())
	require.Error(s.T(), err)
}

func (s *poolTestSuite) TestPool_WrongPassword() {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)

	_, err := NewPoolWithOptions(addr, *testUser, "wrong-password", "",
		WithPoolLimits(5, 10, 5),
		WithLogFunc(log.Debugf),
		WithNewPoolPingTimeout(time.Second),
	)

	require.ErrorContains(s.T(), err, "ERROR 1045 (28000): Access denied for user")
}

func (s *poolTestSuite) TestPool_WrongAddr() {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(s.T(), err)

	laddr, ok := l.Addr().(*net.TCPAddr)
	require.True(s.T(), ok)

	_ = l.Close()

	_, err = NewPoolWithOptions(laddr.String(), *testUser, *testPassword, "",
		WithPoolLimits(5, 10, 5),
		WithLogFunc(log.Debugf),
		WithNewPoolPingTimeout(time.Second),
	)

	require.Error(s.T(), err)
}
