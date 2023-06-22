package client

import (
	"context"
	"fmt"
	"strings"
	"testing"

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
	pool := NewPool(log.Debugf, 5, 10, 5, addr, *testUser, *testPassword, "")
	conn, err := pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	err = conn.Ping()
	require.NoError(s.T(), err)
	pool.PutConn(conn)
	pool.Close()
	var poolStats ConnectionStats
	pool.GetStats(&poolStats)
	require.Equal(s.T(), 0, poolStats.TotalCount)
	require.Len(s.T(), pool.readyConnection, 0)
	_, err = pool.GetConn(context.Background())
	require.Error(s.T(), err)
}
