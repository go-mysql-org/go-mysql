package mysql

import (
	"github.com/pingcap/check"
)

type positionCompareSuite struct {
}

var _ = check.Suite(&positionCompareSuite{})

func (t *positionCompareSuite) TestPosCompare(c *check.C) {
	ascendingPositions := []Position{
		{
			"",
			4,
		},
		{
			"",
			100,
		},
		{
			"mysql-bin.000001",
			4,
		},
		{
			"mysql-bin.000001",
			100,
		},
		{
			"mysql-bin.000002",
			4,
		},
		{
			"mysql-bin.999999",
			4,
		},
		{
			"mysql-bin.1000000",
			4,
		},
	}

	for i := 1; i < len(ascendingPositions); i++ {
		c.Assert(ascendingPositions[i-1].Compare(ascendingPositions[i]), check.Equals, -1)
	}

	for _, p := range ascendingPositions {
		c.Assert(p.Compare(p), check.Equals, 0)
	}
}
