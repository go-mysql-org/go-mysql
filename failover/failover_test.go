package failover

import (
	"flag"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/test_util"
)

var enable_failover_test = flag.Bool("test-failover", false, "enable test failover")

type failoverTestSuite struct {
	suite.Suite
	s []*Server
}

func TestFailoverSuite(t *testing.T) {
	suite.Run(t, new(failoverTestSuite))
}

func (s *failoverTestSuite) SetupSuite() {
	if !*enable_failover_test {
		s.T().Skip("skip test failover")
	}

	ports := []int{3306, 3307, 3308, 3316, 3317, 3318}

	s.s = make([]*Server, len(ports))

	for i := 0; i < len(ports); i++ {
		s.s[i] = NewServer(fmt.Sprintf("%s:%d", *test_util.MysqlHost, ports[i]), User{"root", ""}, User{"root", ""})
	}

	var err error
	for i := 0; i < len(ports); i++ {
		err = s.s[i].StopSlave()
		require.NoError(s.T(), err)

		err = s.s[i].ResetSlaveALL()
		require.NoError(s.T(), err)

		_, err = s.s[i].Execute(`SET GLOBAL BINLOG_FORMAT = "ROW"`)
		require.NoError(s.T(), err)

		_, err = s.s[i].Execute("DROP TABLE IF EXISTS test.go_mysql_test")
		require.NoError(s.T(), err)

		_, err = s.s[i].Execute("CREATE TABLE IF NOT EXISTS test.go_mysql_test (id INT AUTO_INCREMENT, name VARCHAR(256), PRIMARY KEY(id)) engine=innodb")
		require.NoError(s.T(), err)

		err = s.s[i].ResetMaster()
		require.NoError(s.T(), err)
	}
}

func (s *failoverTestSuite) TearDownSuite() {
}

func (s *failoverTestSuite) TestMysqlFailover() {
	h := new(MysqlGTIDHandler)

	m := s.s[0]
	s1 := s.s[1]
	s2 := s.s[2]

	s.testFailover(h, m, s1, s2)
}

func (s *failoverTestSuite) TestMariadbFailover() {
	h := new(MariadbGTIDHandler)

	for i := 3; i <= 5; i++ {
		_, err := s.s[i].Execute("SET GLOBAL gtid_slave_pos = ''")
		require.NoError(s.T(), err)
	}

	m := s.s[3]
	s1 := s.s[4]
	s2 := s.s[5]

	s.testFailover(h, m, s1, s2)
}

func (s *failoverTestSuite) testFailover(h Handler, m *Server, s1 *Server, s2 *Server) {
	var err error
	err = h.ChangeMasterTo(s1, m)
	require.NoError(s.T(), err)

	err = h.ChangeMasterTo(s2, m)
	require.NoError(s.T(), err)

	id := s.checkInsert(m, "a")

	err = h.WaitCatchMaster(s1, m)
	require.NoError(s.T(), err)

	err = h.WaitCatchMaster(s2, m)
	require.NoError(s.T(), err)

	s.checkSelect(s1, id, "a")
	s.checkSelect(s2, id, "a")

	err = s2.StopSlaveIOThread()
	require.NoError(s.T(), err)

	_ = s.checkInsert(m, "b")
	id = s.checkInsert(m, "c")

	err = h.WaitCatchMaster(s1, m)
	require.NoError(s.T(), err)

	s.checkSelect(s1, id, "c")

	best, err := h.FindBestSlaves([]*Server{s1, s2})
	require.NoError(s.T(), err)
	require.Equal(s.T(), []*Server{s1}, best)

	// promote s1 to master
	err = h.Promote(s1)
	require.NoError(s.T(), err)

	// change s2 to master s1
	err = h.ChangeMasterTo(s2, s1)
	require.NoError(s.T(), err)

	err = h.WaitCatchMaster(s2, s1)
	require.NoError(s.T(), err)

	s.checkSelect(s2, id, "c")

	// change m to master s1
	err = h.ChangeMasterTo(m, s1)
	require.NoError(s.T(), err)

	m, s1 = s1, m
	_ = s.checkInsert(m, "d")

	err = h.WaitCatchMaster(s1, m)
	require.NoError(s.T(), err)

	err = h.WaitCatchMaster(s2, m)
	require.NoError(s.T(), err)

	best, err = h.FindBestSlaves([]*Server{s1, s2})
	require.NoError(s.T(), err)
	require.Equal(s.T(), []*Server{s1, s2}, best)

	err = s2.StopSlaveIOThread()
	require.NoError(s.T(), err)

	_ = s.checkInsert(m, "e")
	err = h.WaitCatchMaster(s1, m)
	require.NoError(s.T(), err)

	best, err = h.FindBestSlaves([]*Server{s1, s2})
	require.NoError(s.T(), err)
	require.Equal(s.T(), []*Server{s1}, best)
}

func (s *failoverTestSuite) checkSelect(m *Server, id uint64, name string) {
	rr, err := m.Execute("SELECT name FROM test.go_mysql_test WHERE id = ?", id)
	require.NoError(s.T(), err)
	str, _ := rr.GetString(0, 0)
	require.Equal(s.T(), name, str)
}

func (s *failoverTestSuite) checkInsert(m *Server, name string) uint64 {
	r, err := m.Execute("INSERT INTO test.go_mysql_test (name) VALUES (?)", name)
	require.NoError(s.T(), err)

	return r.InsertId
}
