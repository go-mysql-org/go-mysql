package replication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMariadbGTIDListEvent(t *testing.T) {
	// single GTID, 1-2-3
	data := []byte{1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0}
	ev := MariadbGTIDListEvent{}
	err := ev.Decode(data)
	require.NoError(t, err)
	require.Len(t, ev.GTIDs, 1)
	require.Equal(t, uint32(1), ev.GTIDs[0].DomainID)
	require.Equal(t, uint32(2), ev.GTIDs[0].ServerID)
	require.Equal(t, uint64(3), ev.GTIDs[0].SequenceNumber)

	// multi GTIDs, 1-2-3,4-5-6,7-8-9
	data = []byte{3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0}
	ev = MariadbGTIDListEvent{}
	err = ev.Decode(data)
	require.NoError(t, err)
	require.Len(t, ev.GTIDs, 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, uint32(1+3*i), ev.GTIDs[i].DomainID)
		require.Equal(t, uint32(2+3*i), ev.GTIDs[i].ServerID)
		require.Equal(t, uint64(3+3*i), ev.GTIDs[i].SequenceNumber)
	}
}

func TestMariadbGTIDEvent(t *testing.T) {
	data := []byte{
		1, 2, 3, 4, 5, 6, 7, 8, // SequenceNumber
		0x2a, 1, 0x3b, 4, // DomainID
		0xff,                                           // Flags
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, // commitID
	}
	ev := MariadbGTIDEvent{}
	err := ev.Decode(data)

	require.NoError(t, err)

	require.Equal(t, uint64(0x0807060504030201), ev.GTID.SequenceNumber)
	require.Equal(t, uint32(0x043b012a), ev.GTID.DomainID)
	require.Equal(t, byte(0xff), ev.Flags)
	require.True(t, ev.IsDDL())
	require.True(t, ev.IsStandalone())
	require.True(t, ev.IsGroupCommit())
	require.Equal(t, uint64(0x1716151413121110), ev.CommitID)
	set, err := ev.GTIDNext()
	require.NoError(t, err)
	require.Equal(t, "70975786-0-578437695752307201", set.String())
}

func TestGTIDEventMysql8NewFields(t *testing.T) {
	testcases := []struct {
		data                           []byte
		expectImmediateCommitTimestamp uint64
		expectOriginalCommitTimestamp  uint64
		expectTransactionLength        uint64
		expectImmediateServerVersion   uint32
		expectOriginalServerVersion    uint32
		expectGTID                     string
		expectSequenceNumber           int64
		expectLastCommitted            int64
	}{
		{
			// master: mysql-5.7, slave: mysql-8.0
			data:                           []byte("\x00Z\xa7*\u007fD\xa8\x11\xea\x94\u007f\x02B\xac\x19\x00\x02\x02\x01\x00\x00\x00\x00\x00\x00\x02v\x00\x00\x00\x00\x00\x00\x00w\x00\x00\x00\x00\x00\x00\x00\xc1G\x81\x16x\xa0\x85\x00\x00\x00\x00\x00\x00\x00\xfc\xc5\x03\x938\x01\x80\x00\x00\x00\x00"),
			expectImmediateCommitTimestamp: 1583812517644225,
			expectOriginalCommitTimestamp:  0,
			expectTransactionLength:        965,
			expectImmediateServerVersion:   80019,
			expectOriginalServerVersion:    0,
			expectGTID:                     "5aa72a7f-44a8-11ea-947f-0242ac190002:258",
			expectSequenceNumber:           119,
			expectLastCommitted:            118,
		},
		{
			// mysql-5.7 only
			data:                           []byte("\x00Z\xa7*\u007fD\xa8\x11\xea\x94\u007f\x02B\xac\x19\x00\x02\x03\x01\x00\x00\x00\x00\x00\x00\x025\x00\x00\x00\x00\x00\x00\x006\x00\x00\x00\x00\x00\x00\x00"),
			expectImmediateCommitTimestamp: 0,
			expectOriginalCommitTimestamp:  0,
			expectTransactionLength:        0,
			expectImmediateServerVersion:   0,
			expectOriginalServerVersion:    0,
			expectGTID:                     "5aa72a7f-44a8-11ea-947f-0242ac190002:259",
			expectSequenceNumber:           54,
			expectLastCommitted:            53,
		},
		{
			// mysql-8.0 only
			data:                           []byte("\x00\\\xcc\x103D\xa8\x11\xea\xbdY\x02B\xac\x19\x00\x03w\x00\x00\x00\x00\x00\x00\x00\x02x\x00\x00\x00\x00\x00\x00\x00y\x00\x00\x00\x00\x00\x00\x00j0\xb1>x\xa0\x05\xfc\xc3\x03\x938\x01\x00"),
			expectImmediateCommitTimestamp: 1583813191872618,
			expectOriginalCommitTimestamp:  1583813191872618,
			expectTransactionLength:        963,
			expectImmediateServerVersion:   80019,
			expectOriginalServerVersion:    80019,
			expectGTID:                     "5ccc1033-44a8-11ea-bd59-0242ac190003:119",
			expectSequenceNumber:           121,
			expectLastCommitted:            120,
		},
	}

	for i, tc := range testcases {
		ev := new(GTIDEvent)
		err := ev.Decode(tc.data)
		require.NoError(t, err)
		require.Equal(t, tc.expectImmediateCommitTimestamp, ev.ImmediateCommitTimestamp)
		require.Equal(t, tc.expectOriginalCommitTimestamp, ev.OriginalCommitTimestamp)
		require.Equal(t, tc.expectTransactionLength, ev.TransactionLength)
		require.Equal(t, tc.expectImmediateServerVersion, ev.ImmediateServerVersion)
		require.Equal(t, tc.expectOriginalServerVersion, ev.OriginalServerVersion)
		set, err := ev.GTIDNext()
		require.NoError(t, err)
		assert.Equal(t, tc.expectGTID, set.String(), fmt.Sprintf("testcase: %d", i))
		assert.Equal(t, tc.expectSequenceNumber, ev.SequenceNumber, fmt.Sprintf("testcase: %d", i))
		assert.Equal(t, tc.expectLastCommitted, ev.LastCommitted, fmt.Sprintf("testcase: %d", i))
	}
}

func TestIntVarEvent(t *testing.T) {
	// IntVarEvent Type LastInsertID, Value 13
	data := []byte{1, 13, 0, 0, 0, 0, 0, 0, 0}
	ev := IntVarEvent{}
	err := ev.Decode(data)
	require.NoError(t, err)
	require.Equal(t, LAST_INSERT_ID, ev.Type)
	require.Equal(t, uint64(13), ev.Value)

	// IntVarEvent Type InsertID, Value 23
	data = []byte{2, 23, 0, 0, 0, 0, 0, 0, 0}
	ev = IntVarEvent{}
	err = ev.Decode(data)
	require.NoError(t, err)
	require.Equal(t, INSERT_ID, ev.Type)
	require.Equal(t, uint64(23), ev.Value)
}
