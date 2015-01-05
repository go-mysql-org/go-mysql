package mysql

import (
	"gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type mysqlTestSuite struct {
}

var _ = check.Suite(&mysqlTestSuite{})

func (s *mysqlTestSuite) SetUpSuite(c *check.C) {

}

func (s *mysqlTestSuite) TearDownSuite(c *check.C) {

}

func (t *mysqlTestSuite) TestGTIDInterval(c *check.C) {
	i, err := parseInterval("1-2")
	c.Assert(err, check.IsNil)
	c.Assert(i, check.DeepEquals, Interval{1, 3})

	i, err = parseInterval("1")
	c.Assert(err, check.IsNil)
	c.Assert(i, check.DeepEquals, Interval{1, 2})

	i, err = parseInterval("1-1")
	c.Assert(err, check.IsNil)
	c.Assert(i, check.DeepEquals, Interval{1, 2})

	i, err = parseInterval("1-2")
	c.Assert(err, check.IsNil)
}

func (t *mysqlTestSuite) TestGTIDCodec(c *check.C) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	c.Assert(err, check.IsNil)

	c.Assert(us.String(), check.Equals, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")

	buf := us.Encode()
	err = us.Decode(buf)
	c.Assert(err, check.IsNil)

	gs, err := ParseGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2,de278ad0-2106-11e4-9f8e-6edd0ca20948:1-2")
	c.Assert(err, check.IsNil)

	buf = gs.Encode()
	err = gs.Decode(buf)
	c.Assert(err, check.IsNil)
}
