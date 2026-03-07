package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseUUIDSet(t *testing.T) {
	cases := []struct {
		gtid      string
		intervals IntervalSlice
	}{
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1",
			IntervalSlice{
				Interval{1, 2, ""},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3",
			IntervalSlice{
				Interval{1, 4, ""},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5-8",
			IntervalSlice{
				Interval{1, 4, ""},
				Interval{5, 9, ""},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5:7-8:10",
			IntervalSlice{
				Interval{1, 4, ""},
				Interval{5, 6, ""},
				Interval{7, 9, ""},
				Interval{10, 11, ""},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:foobar:1",
			IntervalSlice{
				Interval{1, 2, "foobar"},
				Interval{1, 4, ""},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:abcdefgh:1:abcdefghijklmnopqrstuvwxyz:1:abcdefghijklmnopqrstuvwxyz_____:1:abcdefghijklmnopqrstuvwxyz_____x:1:foobar:1-2:foobaz:1:x:1",
			IntervalSlice{
				Interval{Start: 1, Stop: 2, Tag: "abcdefgh"},
				Interval{Start: 1, Stop: 2, Tag: "abcdefghijklmnopqrstuvwxyz"},
				Interval{Start: 1, Stop: 2, Tag: "abcdefghijklmnopqrstuvwxyz_____"},
				Interval{Start: 1, Stop: 2, Tag: "abcdefghijklmnopqrstuvwxyz_____x"},
				Interval{Start: 1, Stop: 2, Tag: "foobaz"},
				Interval{Start: 1, Stop: 2, Tag: "x"},
				Interval{Start: 1, Stop: 3, Tag: "foobar"},
				Interval{Start: 1, Stop: 4, Tag: ""},
			},
		},
	}

	for _, tc := range cases {
		uuidset, err := ParseUUIDSet(tc.gtid)
		require.NoError(t, err, tc.gtid)
		require.Equal(t, tc.gtid[0:36], uuidset.SID.String())
		require.Equal(t, tc.intervals.Normalize(), uuidset.Intervals, tc.gtid)
	}
}

func TestParseUUIDSet_Invalid(t *testing.T) {
	cases := []struct {
		gtid string
	}{
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:abcdefghijklmnopqrstuvwxyz_____xy:1"}, // tag too long
	}

	for _, tc := range cases {
		_, err := ParseUUIDSet(tc.gtid)
		require.Error(t, err, tc.gtid)
	}
}
