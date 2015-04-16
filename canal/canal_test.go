package canal

import (
	"flag"
	"testing"

	. "gopkg.in/check.v1"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL host")

func Test(t *testing.T) {
	TestingT(t)
}

type canalTestSuite struct {
}

var _ = Suite(&canalTestSuite{})

func (s *canalTestSuite) SetUpSuite(c *C) {

}

func (s *canalTestSuite) TearDownSuite(c *C) {

}
