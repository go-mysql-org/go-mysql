package canal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Calls made within the same second used to return the same ServerID.
func TestNewDefaultConfigServerID(t *testing.T) {
	ids := make(map[uint32]struct{})

	for range 10 {
		id := NewDefaultConfig().ServerID

		require.GreaterOrEqual(t, id, uint32(1001))
		require.LessOrEqual(t, id, uint32(2000))

		ids[id] = struct{}{}
	}

	require.Greater(t, len(ids), 1, "expected multiple distinct ServerIDs")
}
