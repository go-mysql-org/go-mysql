package mysql

import (
	"github.com/pingcap/check"
)

type resultTestSuite struct {
}

var _ = check.Suite(&resultTestSuite{})

func (t *resultTestSuite) TestLastChained(c *check.C) {
	r1 := &Result{}
	n, last := r1.lastChained()
	c.Assert(last == r1, check.IsTrue)
	c.Assert(n, check.Equals, 1)

	r2 := &Result{}
	r1.ChainResult(r2)
	n, last = r1.lastChained()
	c.Assert(last == r2, check.IsTrue)
	c.Assert(n, check.Equals, 2)

	n, last = r2.lastChained()
	c.Assert(last == r2, check.IsTrue)
	c.Assert(n, check.Equals, 1)

	c.Assert(r1.Length(), check.Equals, 2)
	c.Assert(r2.Length(), check.Equals, 1)
}
