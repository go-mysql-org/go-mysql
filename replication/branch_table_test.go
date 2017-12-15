package replication

import (
    . "github.com/pingcap/check"
)

type testBranchTableSuite struct{}

var _ = Suite(&testBranchTableSuite{})

func (_ *testBranchTableSuite) TestMaxEventType(c *C) {

    c.Assert(int(MaxEventTypeCount), Equals, 256)

}

func (_ *testBranchTableSuite) TestEventTypeString(c *C) {

    for i:=0; i< MaxEventTypeCount; i++ {
        c.Assert(EventType(i).String(), Equals, EventType(i).stringForTest())
    }

}