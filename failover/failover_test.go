package failover

import (
	"flag"
	"fmt"
	. "gopkg.in/check.v1"
	"testing"
)

// We will use go-mysql docker to test
// go-mysql docker will build mysql 1-6 instances
// mysql 1-3 use binlog file + position replication, port is 3306, 3307, 3308
// mysql 4-6 use GTID replication, port is 3316, 3317, 3318
var addr = flag.String("addr", "10.20.151.148", "go-mysql docker container address")

func Test(t *testing.T) {
	TestingT(t)
}

type failoverTestSuite struct {
	s []*Server
}

var _ = Suite(&failoverTestSuite{})

func (s *failoverTestSuite) SetUpSuite(c *C) {
	ports := []int{3306, 3307, 3308, 3316, 3317, 3318}

	s.s = make([]*Server, 6)

	for i := 0; i < 6; i++ {
		s.s[i] = NewServer(fmt.Sprintf("%s:%d", *addr, ports[i]), User{"root", ""}, User{"root", ""})
	}

	var err error
	for i := 0; i < 6; i++ {
		err = s.s[i].StopSlave()
		c.Assert(err, IsNil)

		err = s.s[i].ResetSlave()
		c.Assert(err, IsNil)

		_, err = s.s[i].Execute("DROP TABLE IF EXISTS test.go_mysql_test")
		c.Assert(err, IsNil)

		_, err = s.s[i].Execute("CREATE TABLE IF NOT EXISTS test.go_mysql_test (id INT AUTO_INCREMENT, name VARCHAR(256), PRIMARY KEY(id)) engine=innodb")
		c.Assert(err, IsNil)

		err = s.s[i].ResetMaster()
		c.Assert(err, IsNil)
	}
}

func (s *failoverTestSuite) TearDownSuite(c *C) {
}

func (s *failoverTestSuite) TestGTID(c *C) {
	h := new(GTIDHandler)

	//s3 is master, s4 and s5 are s4's slave
	m := s.s[3]
	s1 := s.s[4]
	s2 := s.s[5]

	var err error
	err = h.ChangeMasterTo(s1, m)
	c.Assert(err, IsNil)

	err = h.ChangeMasterTo(s2, m)
	c.Assert(err, IsNil)

	id := s.checkInsert(c, m, "a")

	err = h.WaitCatchMaster(s1, m)
	c.Assert(err, IsNil)

	err = h.WaitCatchMaster(s2, m)
	c.Assert(err, IsNil)

	s.checkSelect(c, s1, id, "a")
	s.checkSelect(c, s2, id, "a")

	err = s2.StopSlaveIOThread()
	c.Assert(err, IsNil)

	id = s.checkInsert(c, m, "b")
	id = s.checkInsert(c, m, "c")

	err = h.WaitCatchMaster(s1, m)
	c.Assert(err, IsNil)

	s.checkSelect(c, s1, id, "c")

	s.checkCompare(c, h, s1, s2, 1)
	s.checkCompare(c, h, s2, s1, -1)

	best, err := h.FindBestSlaves([]*Server{s1, s2})
	c.Assert(err, IsNil)
	c.Assert(best, DeepEquals, []*Server{s1})

	// promote s1 to master
	err = h.Promote(s1)
	c.Assert(err, IsNil)

	// change s2 to master s1
	err = h.ChangeMasterTo(s2, s1)
	c.Assert(err, IsNil)

	err = h.WaitCatchMaster(s2, s1)
	c.Assert(err, IsNil)

	s.checkSelect(c, s2, id, "c")

	// change m to master s1
	err = h.ChangeMasterTo(m, s1)
	c.Assert(err, IsNil)

	m, s1 = s1, m
	id = s.checkInsert(c, m, "d")

	err = h.WaitCatchMaster(s1, m)
	c.Assert(err, IsNil)

	err = h.WaitCatchMaster(s2, m)
	c.Assert(err, IsNil)

	s.checkCompare(c, h, s1, s2, 0)

	best, err = h.FindBestSlaves([]*Server{s1, s2})
	c.Assert(err, IsNil)
	c.Assert(best, DeepEquals, []*Server{s1, s2})

	err = s2.StopSlaveIOThread()
	c.Assert(err, IsNil)

	id = s.checkInsert(c, m, "e")
	err = h.WaitCatchMaster(s1, m)

	s.checkCompare(c, h, s1, s2, 1)

	best, err = h.FindBestSlaves([]*Server{s1, s2})
	c.Assert(err, IsNil)
	c.Assert(best, DeepEquals, []*Server{s1})
}

func (s *failoverTestSuite) checkSelect(c *C, m *Server, id uint64, name string) {
	rr, err := m.Execute("SELECT name FROM test.go_mysql_test WHERE id = ?", id)
	c.Assert(err, IsNil)
	str, _ := rr.GetString(0, 0)
	c.Assert(str, Equals, name)
}

func (s *failoverTestSuite) checkInsert(c *C, m *Server, name string) uint64 {
	r, err := m.Execute("INSERT INTO test.go_mysql_test (name) VALUES (?)", name)
	c.Assert(err, IsNil)

	return r.InsertId
}

func (s *failoverTestSuite) checkCompare(c *C, h Handler, s1 *Server, s2 *Server, cv int) {
	v, err := h.Compare(s1, s2)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, cv)
}
