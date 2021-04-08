package replication

import (
	. "github.com/pingcap/check"
)

func (_ *testDecodeSuite) TestMariadbGTIDListEvent(c *C) {
	// single GTID, 1-2-3
	data := []byte{1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0}
	ev := MariadbGTIDListEvent{}
	err := ev.Decode(data)
	c.Assert(err, IsNil)
	c.Assert(len(ev.GTIDs), Equals, 1)
	c.Assert(ev.GTIDs[0].DomainID, Equals, uint32(1))
	c.Assert(ev.GTIDs[0].ServerID, Equals, uint32(2))
	c.Assert(ev.GTIDs[0].SequenceNumber, Equals, uint64(3))

	// multi GTIDs, 1-2-3,4-5-6,7-8-9
	data = []byte{3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0}
	ev = MariadbGTIDListEvent{}
	err = ev.Decode(data)
	c.Assert(err, IsNil)
	c.Assert(len(ev.GTIDs), Equals, 3)
	for i := 0; i < 3; i++ {
		c.Assert(ev.GTIDs[i].DomainID, Equals, uint32(1+3*i))
		c.Assert(ev.GTIDs[i].ServerID, Equals, uint32(2+3*i))
		c.Assert(ev.GTIDs[i].SequenceNumber, Equals, uint64(3+3*i))
	}
}

func (_ *testDecodeSuite) TestMariadbGTIDEvent(c *C) {
	data := []byte{
		1, 2, 3, 4, 5, 6, 7, 8, // SequenceNumber
		0x2a, 1, 0x3b, 4, // DomainID
		0xff,                                           // Flags
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, // commitID
	}
	ev := MariadbGTIDEvent{}
	err := ev.Decode(data)

	c.Assert(err, IsNil)

	c.Assert(ev.GTID.SequenceNumber, Equals, uint64(0x0807060504030201))
	c.Assert(ev.GTID.DomainID, Equals, uint32(0x043b012a))
	c.Assert(ev.Flags, Equals, byte(0xff))
	c.Assert(ev.IsDDL(), Equals, true)
	c.Assert(ev.IsStandalone(), Equals, true)
	c.Assert(ev.IsGroupCommit(), Equals, true)
	c.Assert(ev.CommitID, Equals, uint64(0x1716151413121110))
}

func (_ *testDecodeSuite) TestGTIDEventMysql8NewFields(c *C) {
	testcases := []struct {
		data                           []byte
		expectImmediateCommitTimestamp uint64
		expectOriginalCommitTimestamp  uint64
		expectTransactoinLength        uint64
		expectImmediateServerVersion   uint32
		expectOriginalServerVersion    uint32
	}{
		{
			// master: mysql-5.7, slave: mysql-8.0
			data:                           []byte("\x00Z\xa7*\u007fD\xa8\x11\xea\x94\u007f\x02B\xac\x19\x00\x02\x02\x01\x00\x00\x00\x00\x00\x00\x02v\x00\x00\x00\x00\x00\x00\x00w\x00\x00\x00\x00\x00\x00\x00\xc1G\x81\x16x\xa0\x85\x00\x00\x00\x00\x00\x00\x00\xfc\xc5\x03\x938\x01\x80\x00\x00\x00\x00"),
			expectImmediateCommitTimestamp: 1583812517644225,
			expectOriginalCommitTimestamp:  0,
			expectTransactoinLength:        965,
			expectImmediateServerVersion:   80019,
			expectOriginalServerVersion:    0,
		},
		{
			// mysql-5.7 only
			data:                           []byte("\x00Z\xa7*\u007fD\xa8\x11\xea\x94\u007f\x02B\xac\x19\x00\x02\x03\x01\x00\x00\x00\x00\x00\x00\x025\x00\x00\x00\x00\x00\x00\x006\x00\x00\x00\x00\x00\x00\x00"),
			expectImmediateCommitTimestamp: 0,
			expectOriginalCommitTimestamp:  0,
			expectTransactoinLength:        0,
			expectImmediateServerVersion:   0,
			expectOriginalServerVersion:    0,
		},
		{
			// mysql-8.0 only
			data:                           []byte("\x00\\\xcc\x103D\xa8\x11\xea\xbdY\x02B\xac\x19\x00\x03w\x00\x00\x00\x00\x00\x00\x00\x02x\x00\x00\x00\x00\x00\x00\x00y\x00\x00\x00\x00\x00\x00\x00j0\xb1>x\xa0\x05\xfc\xc3\x03\x938\x01\x00"),
			expectImmediateCommitTimestamp: 1583813191872618,
			expectOriginalCommitTimestamp:  1583813191872618,
			expectTransactoinLength:        963,
			expectImmediateServerVersion:   80019,
			expectOriginalServerVersion:    80019,
		},
	}

	for _, tc := range testcases {
		ev := new(GTIDEvent)
		err := ev.Decode(tc.data)
		c.Assert(err, IsNil)
		c.Assert(ev.ImmediateCommitTimestamp, Equals, tc.expectImmediateCommitTimestamp)
		c.Assert(ev.OriginalCommitTimestamp, Equals, tc.expectOriginalCommitTimestamp)
		c.Assert(ev.ImmediateServerVersion, Equals, tc.expectImmediateServerVersion)
		c.Assert(ev.OriginalServerVersion, Equals, tc.expectOriginalServerVersion)
	}
}
