package mysql

import (
	"github.com/pingcap/check"
)

type utilTestSuite struct {
}

var _ = check.Suite(&utilTestSuite{})

func (s *utilTestSuite) TestCompareServerVersions(c *check.C) {
	tests := []struct {
		A      string
		B      string
		Expect int
	}{
		{A: "1.2.3", B: "1.2.3", Expect: 0},
		{A: "5.6-999", B: "8.0", Expect: -1},
		{A: "8.0.32-0ubuntu0.20.04.2", B: "8.0.28", Expect: 1},
	}

	for _, test := range tests {
		comment := check.Commentf("%q vs. %q", test.A, test.B)

		got, err := CompareServerVersions(test.A, test.B)
		c.Assert(err, check.IsNil, comment)
		c.Assert(got, check.Equals, test.Expect, comment)
	}
}
