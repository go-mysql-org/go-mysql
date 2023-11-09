package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPosCompare(t *testing.T) {
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
		require.Equal(t, -1, ascendingPositions[i-1].Compare(ascendingPositions[i]))
	}

	for _, p := range ascendingPositions {
		require.Equal(t, 0, p.Compare(p))
	}
}
