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
