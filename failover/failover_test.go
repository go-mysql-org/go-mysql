package failover

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type failoverTestSuite struct {
}

var _ = Suite(&failoverTestSuite{})

func (s *failoverTestSuite) SetUpSuite(c *C) {
}

func (s *failoverTestSuite) TearDownSuite(c *C) {
}
