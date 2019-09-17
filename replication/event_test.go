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
