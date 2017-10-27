package mysql

import (
	"github.com/pingcap/check"
)

type mariaDBTestSuite struct {
}

var _ = check.Suite(&mariaDBTestSuite{})

func (t *mariaDBTestSuite) SetUpSuite(c *check.C) {

}

func (t *mariaDBTestSuite) TearDownSuite(c *check.C) {

}

func (t *mariaDBTestSuite) TestParseMariadbGTIDSet(c *check.C) {
	g1, err := ParseMariadbGTIDSet("0-1-1")
	c.Assert(err, check.IsNil)

	c.Assert(g1.String(), check.Equals, "0-1-1")
}

func (t *mariaDBTestSuite) TestMariaDBEqual(c *check.C) {
	g1, err := ParseMariadbGTIDSet("0-1-1")
	c.Assert(err, check.IsNil)

	g2, err := ParseMariadbGTIDSet("0-1-1")
	c.Assert(err, check.IsNil)

	g3, err := ParseMariadbGTIDSet("0-1-2")
	c.Assert(err, check.IsNil)

	c.Assert(g1.Equal(g2), check.IsTrue)
	c.Assert(g1.Equal(g3), check.IsFalse)
}

func (t *mariaDBTestSuite) TestMariaDBUpdate(c *check.C) {
	g1, err := ParseMariadbGTIDSet("0-1-1")
	c.Assert(err, check.IsNil)

	g1.Update("0-1-2")

	c.Assert(g1.String(), check.Equals, "0-1-2")
}
