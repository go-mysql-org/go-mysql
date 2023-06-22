package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTime(tt *testing.T) {
	tbls := []struct {
		year     int
		month    int
		day      int
		hour     int
		min      int
		sec      int
		microSec int
		frac     int
		expected string
	}{
		{2000, 1, 1, 1, 1, 1, 1, 0, "2000-01-01 01:01:01"},
		{2000, 1, 1, 1, 1, 1, 1, 1, "2000-01-01 01:01:01.0"},
		{2000, 1, 1, 1, 1, 1, 1, 6, "2000-01-01 01:01:01.000001"},
	}

	for _, t := range tbls {
		t1 := fracTime{time.Date(t.year, time.Month(t.month), t.day, t.hour, t.min, t.sec, t.microSec*1000, time.UTC), t.frac, nil}
		require.Equal(tt, t.expected, t1.String())
	}

	zeroTbls := []struct {
		frac     int
		dec      int
		expected string
	}{
		{0, 1, "0000-00-00 00:00:00.0"},
		{1, 1, "0000-00-00 00:00:00.0"},
		{123, 3, "0000-00-00 00:00:00.000"},
		{123000, 3, "0000-00-00 00:00:00.123"},
		{123, 6, "0000-00-00 00:00:00.000123"},
		{123000, 6, "0000-00-00 00:00:00.123000"},
	}

	for _, t := range zeroTbls {
		require.Equal(tt, t.expected, formatZeroTime(t.frac, t.dec))
	}
}

func TestTimeStringLocation(tt *testing.T) {
	t := fracTime{
		time.Date(2018, time.Month(7), 30, 10, 0, 0, 0, time.FixedZone("EST", -5*3600)),
		0,
		nil,
	}

	require.Equal(tt, "2018-07-30 10:00:00", t.String())

	t = fracTime{
		time.Date(2018, time.Month(7), 30, 10, 0, 0, 0, time.FixedZone("EST", -5*3600)),
		0,
		time.UTC,
	}
	require.Equal(tt, "2018-07-30 15:00:00", t.String())
}
