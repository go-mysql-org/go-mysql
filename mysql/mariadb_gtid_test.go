package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMariaDBGTID(t *testing.T) {
	cases := []struct {
		gtidStr   string
		hashError bool
	}{
		{"0-1-1", false},
		{"", false},
		{"0-1-1-1", true},
		{"1", true},
		{"0-1-seq", true},
	}

	for _, cs := range cases {
		gtid, err := ParseMariadbGTID(cs.gtidStr)
		if cs.hashError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, cs.gtidStr, gtid.String())
		}
	}
}

func TestMariaDBGTIDConatin(t *testing.T) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		contain                     bool
	}{
		{"0-1-1", "0-1-2", false},
		{"0-1-1", "", true},
		{"2-1-1", "1-1-1", false},
		{"1-2-1", "1-1-1", true},
		{"1-2-2", "1-1-1", true},
	}

	for _, cs := range cases {
		originGTID, err := ParseMariadbGTID(cs.originGTIDStr)
		require.NoError(t, err)
		otherGTID, err := ParseMariadbGTID(cs.otherGTIDStr)
		require.NoError(t, err)

		require.Equal(t, cs.contain, originGTID.Contain(otherGTID))
	}
}

func TestMariaDBGTIDClone(t *testing.T) {
	gtid, err := ParseMariadbGTID("1-1-1")
	require.NoError(t, err)

	clone := gtid.Clone()
	require.Equal(t, gtid, clone)
}

func TestMariaDBForward(t *testing.T) {
	cases := []struct {
		currentGTIDStr, newerGTIDStr string
		hashError                    bool
	}{
		{"0-1-1", "0-1-2", false},
		{"0-1-1", "", false},
		{"2-1-1", "1-1-1", true},
		{"1-2-1", "1-1-1", false},
		{"1-2-2", "1-1-1", false},
	}

	for _, cs := range cases {
		currentGTID, err := ParseMariadbGTID(cs.currentGTIDStr)
		require.NoError(t, err)
		newerGTID, err := ParseMariadbGTID(cs.newerGTIDStr)
		require.NoError(t, err)

		err = currentGTID.forward(newerGTID)
		if cs.hashError {
			require.Error(t, err)
			require.Equal(t, cs.currentGTIDStr, currentGTID.String())
		} else {
			require.NoError(t, err)
			require.Equal(t, cs.newerGTIDStr, currentGTID.String())
		}
	}
}

func TestParseMariaDBGTIDSet(t *testing.T) {
	cases := []struct {
		gtidStr     string
		subGTIDs    map[uint32]string // domain ID => gtid string
		expectedStr string
		hasError    bool
	}{
		{"0-1-1", map[uint32]string{0: "0-1-1"}, "0-1-1", false},
		{"", nil, "", false},
		{"0-1-1,1-2-3", map[uint32]string{0: "0-1-1", 1: "1-2-3"}, "0-1-1,1-2-3", false},
		{"0-1--1", nil, "", true},
		// Same domain, different server — last one wins (forward replaces)
		{"0-1-1,0-2-2", map[uint32]string{0: "0-2-2"}, "0-2-2", false},
	}

	for _, cs := range cases {
		gtidSet, err := ParseMariadbGTIDSet(cs.gtidStr)
		if cs.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			mariadbGTIDSet, ok := gtidSet.(*MariadbGTIDSet)
			require.True(t, ok)

			// check sub gtid
			require.Len(t, mariadbGTIDSet.Sets, len(cs.subGTIDs))
			for domainID, gtid := range mariadbGTIDSet.Sets {
				require.Contains(t, cs.subGTIDs, domainID)
				require.Equal(t, cs.subGTIDs[domainID], gtid.String())
			}

			// check String() function
			require.Equal(t, cs.expectedStr, mariadbGTIDSet.String())
		}
	}
}

func TestMariaDBGTIDSetUpdate(t *testing.T) {
	cases := []struct {
		isNilGTID bool
		gtidStr   string
		subGTIDs  map[uint32]string // domain ID => gtid string
	}{
		{true, "", map[uint32]string{1: "1-1-1", 2: "2-2-2"}},
		// Same domain (1), different server (2) — forward replaces server ID
		{false, "1-2-2", map[uint32]string{1: "1-2-2", 2: "2-2-2"}},
		{false, "1-2-1", map[uint32]string{1: "1-2-1", 2: "2-2-2"}},
		{false, "3-2-1", map[uint32]string{1: "1-1-1", 2: "2-2-2", 3: "3-2-1"}},
		{false, "3-2-1,4-2-1", map[uint32]string{1: "1-1-1", 2: "2-2-2", 3: "3-2-1", 4: "4-2-1"}},
	}

	for _, cs := range cases {
		gtidSet, err := ParseMariadbGTIDSet("1-1-1,2-2-2")
		require.NoError(t, err)
		mariadbGTIDSet, ok := gtidSet.(*MariadbGTIDSet)
		require.True(t, ok)

		if cs.isNilGTID {
			require.NoError(t, mariadbGTIDSet.AddSet(nil))
		} else {
			err := gtidSet.Update(cs.gtidStr)
			require.NoError(t, err)
		}
		// check sub gtid
		require.Len(t, mariadbGTIDSet.Sets, len(cs.subGTIDs))
		for domainID, gtid := range mariadbGTIDSet.Sets {
			require.Contains(t, cs.subGTIDs, domainID)
			require.Equal(t, cs.subGTIDs[domainID], gtid.String())
		}
	}
}

func TestMariaDBGTIDSetEqual(t *testing.T) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		equals                      bool
	}{
		{"", "", true},
		{"1-1-1", "1-1-1,2-2-2", false},
		{"1-1-1,2-2-2", "1-1-1", false},
		{"1-1-1,2-2-2", "1-1-1,2-2-2", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-3", false},
		{"0-1-1,0-2-2", "0-2-2", true}, // same domain, forward replaces
	}

	for _, cs := range cases {
		originGTID, err := ParseMariadbGTIDSet(cs.originGTIDStr)
		require.NoError(t, err)

		otherGTID, err := ParseMariadbGTIDSet(cs.otherGTIDStr)
		require.NoError(t, err)

		require.Equal(t, cs.equals, originGTID.Equal(otherGTID))
	}
}

func TestMariaDBGTIDSetContain(t *testing.T) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		contain                     bool
	}{
		{"", "", true},
		{"1-1-1", "1-1-1,2-2-2", false},
		{"1-1-1,2-2-2", "1-1-1", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-2", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-1", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-3", false},
	}

	for _, cs := range cases {
		originGTIDSet, err := ParseMariadbGTIDSet(cs.originGTIDStr)
		require.NoError(t, err)

		otherGTIDSet, err := ParseMariadbGTIDSet(cs.otherGTIDStr)
		require.NoError(t, err)

		require.Equal(t, cs.contain, originGTIDSet.Contain(otherGTIDSet))
	}
}

func TestMariaDBGTIDSetClone(t *testing.T) {
	cases := []string{"", "1-1-1", "1-1-1,2-2-2"}

	for _, str := range cases {
		gtidSet, err := ParseMariadbGTIDSet(str)
		require.NoError(t, err)

		require.Equal(t, gtidSet, gtidSet.Clone())
	}
}

func TestMariaDBGTIDSetSortedString(t *testing.T) {
	cases := [][]string{
		{"", ""},
		{"1-1-1", "1-1-1"},
		{"2-2-2,1-1-1,3-2-1", "1-1-1,2-2-2,3-2-1"},
	}

	for _, strs := range cases {
		gtidSet, err := ParseMariadbGTIDSet(strs[0])
		require.NoError(t, err)
		require.Equal(t, strs[1], gtidSet.String())
	}
}

// TestMariaDBGTIDSetSameDomainDifferentServer tests the key behavior: when a GTID
// with the same domain but different server ID is added (e.g. primary failover),
// the old entry is replaced via forward(), maintaining one position per domain.
func TestMariaDBGTIDSetSameDomainDifferentServer(t *testing.T) {
	// Start with domain 0, server 1013
	gtidSet, err := ParseMariadbGTIDSet("0-1013-100")
	require.NoError(t, err)
	require.Equal(t, "0-1013-100", gtidSet.String())

	// Simulate primary failover: new server 963, higher sequence
	err = gtidSet.Update("0-963-200")
	require.NoError(t, err)
	require.Equal(t, "0-963-200", gtidSet.String(), "should replace server 1013 with 963 in domain 0")

	// Verify only one entry per domain
	mariadbSet := gtidSet.(*MariadbGTIDSet)
	require.Len(t, mariadbSet.Sets, 1)
	require.Equal(t, uint32(963), mariadbSet.Sets[0].ServerID)
	require.Equal(t, uint64(200), mariadbSet.Sets[0].SequenceNumber)

	// Multiple domains: only affected domain is updated
	gtidSet2, err := ParseMariadbGTIDSet("0-1013-100,1-500-50,2-600-75")
	require.NoError(t, err)

	err = gtidSet2.Update("0-963-200")
	require.NoError(t, err)
	require.Equal(t, "0-963-200,1-500-50,2-600-75", gtidSet2.String(), "only domain 0 should change")

	mariadbSet2 := gtidSet2.(*MariadbGTIDSet)
	require.Len(t, mariadbSet2.Sets, 3)
	require.Equal(t, uint32(963), mariadbSet2.Sets[0].ServerID)
	require.Equal(t, uint32(500), mariadbSet2.Sets[1].ServerID, "domain 1 unchanged")
	require.Equal(t, uint32(600), mariadbSet2.Sets[2].ServerID, "domain 2 unchanged")

	// AddSet directly (as binlogsyncer does)
	gtidSet3, err := ParseMariadbGTIDSet("0-1013-100")
	require.NoError(t, err)
	newGTID := &MariadbGTID{DomainID: 0, ServerID: 963, SequenceNumber: 200}
	err = gtidSet3.(*MariadbGTIDSet).AddSet(newGTID)
	require.NoError(t, err)
	require.Equal(t, "0-963-200", gtidSet3.String())

	// Equal after forward: both should be equal
	a, _ := ParseMariadbGTIDSet("0-1013-100")
	b, _ := ParseMariadbGTIDSet("0-963-100")
	require.NoError(t, a.Update("0-963-100"))
	require.True(t, a.Equal(b), "after forward, positions should be equal")

	// Clone preserves flat structure
	gtidSet4, _ := ParseMariadbGTIDSet("0-963-200,1-500-50")
	cloned := gtidSet4.Clone()
	require.True(t, gtidSet4.Equal(cloned))
	// Mutating clone should not affect original
	require.NoError(t, cloned.Update("0-111-300"))
	require.Equal(t, "0-963-200,1-500-50", gtidSet4.String(), "original should be unmodified after clone mutation")
}

func TestMariadbGTIDSetIsEmpty(t *testing.T) {
	emptyGTIDSet := new(MariadbGTIDSet)
	emptyGTIDSet.Sets = make(map[uint32]*MariadbGTID)
	require.True(t, emptyGTIDSet.IsEmpty())

	nonEmptyGTIDSet, err := ParseMariadbGTIDSet("0-1-2")
	require.NoError(t, err)
	require.False(t, nonEmptyGTIDSet.IsEmpty())
}
