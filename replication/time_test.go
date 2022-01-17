package replication

import (
	"time"

	. "github.com/pingcap/check"
)

type testTimeSuite struct{}

var _ = Suite(&testTimeSuite{})

func (s *testTimeSuite) TestTime(c *C) {
	tbls := []struct {
		expected string
		year     int
		month    int
		day      int
		hour     int
		min      int
		sec      int
		microSec int
		frac     int
	}{
		{year: 2000, month: 1, day: 1, hour: 1, min: 1, sec: 1, microSec: 1, frac: 0, expected: "2000-01-01 01:01:01"},
		{year: 2000, month: 1, day: 1, hour: 1, min: 1, sec: 1, microSec: 1, frac: 1, expected: "2000-01-01 01:01:01.0"},
		{year: 2000, month: 1, day: 1, hour: 1, min: 1, sec: 1, microSec: 1, frac: 6, expected: "2000-01-01 01:01:01.000001"},
	}

	for _, t := range tbls {
		t1 := fracTime{time.Date(t.year, time.Month(t.month), t.day, t.hour, t.min, t.sec, t.microSec*1000, time.UTC), nil, t.frac}
		c.Assert(t1.String(), Equals, t.expected)
	}

	zeroTbls := []struct {
		expected string
		frac     int
		dec      int
	}{
		{frac: 0, dec: 1, expected: "0000-00-00 00:00:00.0"},
		{frac: 1, dec: 1, expected: "0000-00-00 00:00:00.0"},
		{frac: 123, dec: 3, expected: "0000-00-00 00:00:00.000"},
		{frac: 123000, dec: 3, expected: "0000-00-00 00:00:00.123"},
		{frac: 123, dec: 6, expected: "0000-00-00 00:00:00.000123"},
		{frac: 123000, dec: 6, expected: "0000-00-00 00:00:00.123000"},
	}

	for _, t := range zeroTbls {
		c.Assert(formatZeroTime(t.frac, t.dec), Equals, t.expected)
	}
}

func (s *testTimeSuite) TestTimeStringLocation(c *C) {
	t := fracTime{
		Time:                    time.Date(2018, time.Month(7), 30, 10, 0, 0, 0, time.FixedZone("EST", -5*3600)),
		Dec:                     0,
		timestampStringLocation: nil,
	}

	c.Assert(t.String(), Equals, "2018-07-30 10:00:00")

	t = fracTime{
		Time:                    time.Date(2018, time.Month(7), 30, 10, 0, 0, 0, time.FixedZone("EST", -5*3600)),
		Dec:                     0,
		timestampStringLocation: time.UTC,
	}
	c.Assert(t.String(), Equals, "2018-07-30 15:00:00")
}

var _ = Suite(&testTimeSuite{})
