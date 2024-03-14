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
		subGTIDs    map[uint32]map[uint32]string //domain ID => gtid string
		expectedStr []string                     // test String()
		hasError    bool
	}{
		{"0-1-1", map[uint32]map[uint32]string{0: {1: "0-1-1"}}, []string{"0-1-1"}, false},
		{"", nil, []string{""}, false},
		{"0-1-1,1-2-3", map[uint32]map[uint32]string{0: {1: "0-1-1"}, 1: {2: "1-2-3"}}, []string{"0-1-1,1-2-3", "1-2-3,0-1-1"}, false},
		{"0-1--1", nil, nil, true},
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
			for domainID, set := range mariadbGTIDSet.Sets {
				require.Contains(t, mariadbGTIDSet.Sets, domainID)
				for serverID, gtid := range set {
					require.Contains(t, cs.subGTIDs, domainID)
					require.Equal(t, cs.subGTIDs[domainID][serverID], gtid.String())
				}
			}

			// check String() function
			inExpectedResult := false
			actualStr := mariadbGTIDSet.String()
			for _, str := range cs.expectedStr {
				if str == actualStr {
					inExpectedResult = true
					break
				}
			}
			require.True(t, inExpectedResult)
		}
	}
}

func TestMariaDBGTIDSetUpdate(t *testing.T) {
	cases := []struct {
		isNilGTID bool
		gtidStr   string
		subGTIDs  map[uint32]map[uint32]string
	}{
		{true, "", map[uint32]map[uint32]string{1: {1: "1-1-1"}, 2: {2: "2-2-2"}}},
		{false, "1-2-2", map[uint32]map[uint32]string{1: {1: "1-1-1", 2: "1-2-2"}, 2: {2: "2-2-2"}}},
		{false, "1-2-1", map[uint32]map[uint32]string{1: {1: "1-1-1", 2: "1-2-1"}, 2: {2: "2-2-2"}}},
		{false, "3-2-1", map[uint32]map[uint32]string{1: {1: "1-1-1"}, 2: {2: "2-2-2"}, 3: {2: "3-2-1"}}},
		{false, "3-2-1,4-2-1", map[uint32]map[uint32]string{1: {1: "1-1-1"}, 2: {2: "2-2-2"}, 3: {2: "3-2-1"}, 4: {2: "4-2-1"}}},
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
		for domainID, set := range mariadbGTIDSet.Sets {
			require.Contains(t, mariadbGTIDSet.Sets, domainID)
			for serverID, gtid := range set {
				require.Contains(t, cs.subGTIDs, domainID)
				require.Equal(t, cs.subGTIDs[domainID][serverID], gtid.String())
			}
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
		{"0-1-1,0-2-2", "0-2-2", false},
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
	cases := [][]string{{"", ""}, {"1-1-1", "1-1-1"},
		{"2-2-2,1-1-1,3-2-1", "1-1-1,2-2-2,3-2-1"}}

	for _, strs := range cases {
		gtidSet, err := ParseMariadbGTIDSet(strs[0])
		require.NoError(t, err)
		require.Equal(t, strs[1], gtidSet.String())
	}
}
