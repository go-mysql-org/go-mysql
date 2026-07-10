package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTagRegexp(t *testing.T) {
	cases := []struct {
		tag string
		ok  bool
	}{
		{"_", true},
		{"_abcdefghi_abcdefghi", true},
		{"_abcdefghi_abcdefghi_abcdefghi_a", true},
		{"_abcdefghi_abcdefghi_abcdefghi_ab", false}, // too long
		{"foo", true},
		{"Foo", true},      // upper case is allowed before normalization
		{"foo-bar", false}, // character not allowed
		{"foo123bar", true},
		{"123bar", false}, // can't start with a number
		{" foo ", true},   // leading/trailing whitspace is allowed before normalization
		{"", false},       // too short
	}

	for _, tc := range cases {
		r := tagRegexp.MatchString(tc.tag)
		require.Equal(t, tc.ok, r, tc.tag)
	}
}

func TestParseInterval(t *testing.T) {
	i, err := parseInterval("1-2")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 3}, i)

	i, err = parseInterval("1")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 2}, i)

	i, err = parseInterval("1-1")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 2}, i)

	_, err = parseInterval("x")
	require.Error(t, err)

	_, err = parseInterval("1-x")
	require.Error(t, err)

	_, err = parseInterval("x-2")
	require.Error(t, err)

	_, err = parseInterval("3-2")
	require.Error(t, err)
}

func FuzzParseInterval(f *testing.F) {
	cases := []string{
		"1-2",
		"1-100",
		"1-100",
	}
	for _, tc := range cases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, str string) {
		_, _ = parseInterval(str)
	})
}

func TestIntervalSlice(t *testing.T) {
	i := IntervalSlice{Interval{1, 2}, Interval{2, 4}, Interval{2, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 2}, Interval{2, 3}, Interval{2, 4}}, i)
	n := i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 4}}, n)

	i = IntervalSlice{Interval{1, 2}, Interval{3, 5}, Interval{1, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 2}, Interval{1, 3}, Interval{3, 5}}, i)
	n = i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 5}}, n)

	i = IntervalSlice{Interval{1, 2}, Interval{4, 5}, Interval{1, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 2}, Interval{1, 3}, Interval{4, 5}}, i)
	n = i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 3}, Interval{4, 5}}, n)

	i = IntervalSlice{Interval{1, 4}, Interval{2, 3}}
	i.Sort()
	require.Equal(t, IntervalSlice{Interval{1, 4}, Interval{2, 3}}, i)
	n = i.Normalize()
	require.Equal(t, IntervalSlice{Interval{1, 4}}, n)

	n1 := IntervalSlice{Interval{1, 3}, Interval{4, 5}}
	n2 := IntervalSlice{Interval{1, 2}}

	require.True(t, n1.Contain(n2))
	require.False(t, n2.Contain(n1))

	n1 = IntervalSlice{Interval{1, 3}, Interval{4, 5}}
	n2 = IntervalSlice{Interval{1, 6}}

	require.False(t, n1.Contain(n2))
	require.True(t, n2.Contain(n1))
}

func TestIntervalSlice_Contain(t *testing.T) {
	cases := []struct {
		sliceA    IntervalSlice
		sliceB    IntervalSlice
		contained bool
	}{
		{
			IntervalSlice{},
			IntervalSlice{},
			true,
		},
		{
			IntervalSlice{
				Interval{Start: 1, Stop: 3},
			},
			IntervalSlice{
				Interval{Start: 1, Stop: 3},
			},
			true,
		},
		{
			IntervalSlice{
				Interval{Start: 1, Stop: 4},
			},
			IntervalSlice{
				Interval{Start: 1, Stop: 3},
			},
			true,
		},
		{
			IntervalSlice{
				Interval{Start: 1, Stop: 3},
			},
			IntervalSlice{
				Interval{Start: 1, Stop: 4},
			},
			false,
		},
		{
			IntervalSlice{
				Interval{Start: 1, Stop: 3},
			},
			IntervalSlice{
				Interval{Start: 5, Stop: 20},
			},
			false,
		},
	}

	for _, tc := range cases {
		c := tc.sliceA.Contain(tc.sliceB)
		require.Equal(t, tc.contained, c, "%s contains %s: expected %v", tc.sliceA, tc.sliceB, tc.contained)
	}
}

// TestIntervalSlice_Contain_UnsortedSub asserts that Contain returns the
// correct result regardless of whether sub is sorted. Both subs below cover
// the same set of values ({2, 12}), and both are subsets of s, so Contain
// must return true for both.
func TestIntervalSlice_Contain_UnsortedSub(t *testing.T) {
	s := IntervalSlice{
		Interval{Start: 1, Stop: 3},
		Interval{Start: 10, Stop: 15},
	}
	sortedSub := IntervalSlice{
		Interval{Start: 2, Stop: 3},
		Interval{Start: 12, Stop: 13},
	}
	unsortedSub := IntervalSlice{
		Interval{Start: 12, Stop: 13},
		Interval{Start: 2, Stop: 3},
	}

	require.True(t, s.Contain(sortedSub))
	require.True(t, s.Contain(unsortedSub))
}

func TestIntervalSlice_Equal(t *testing.T) {
	cases := []struct {
		sliceA IntervalSlice
		sliceB IntervalSlice
		equal  bool
	}{
		{
			IntervalSlice{
				Interval{Start: 10, Stop: 30},
			},
			IntervalSlice{
				Interval{Start: 10, Stop: 30},
			},
			true,
		},
		{
			IntervalSlice{
				Interval{Start: 10, Stop: 30},
			},
			IntervalSlice{
				Interval{Start: 10, Stop: 30},
				Interval{Start: 40, Stop: 42},
			},
			false,
		},
		{
			IntervalSlice{
				Interval{Start: 10, Stop: 30},
			},
			IntervalSlice{
				Interval{Start: 11, Stop: 30},
			},
			false,
		},
		{
			IntervalSlice{
				Interval{Start: 10, Stop: 30},
			},
			IntervalSlice{
				Interval{Start: 10, Stop: 31},
			},
			false,
		},
	}

	for _, tc := range cases {
		c := tc.sliceA.Equal(tc.sliceB)
		require.Equal(t, tc.equal, c, "%s equals %s: expected %v", tc.sliceA, tc.sliceB, tc.equal)
	}
}

func TestIntervalSlice_Len(t *testing.T) {
	is := IntervalSlice{
		Interval{Start: 1, Stop: 6},
		Interval{Start: 1, Stop: 5},
	}
	require.Equal(t, 2, is.Len())
}

func TestIntervalSlice_Normalize(t *testing.T) {
	i1 := IntervalSlice{}
	i2 := i1.Normalize()
	require.Equal(t, i1, i2)

	var i3 IntervalSlice
	i4 := i3.Normalize()
	require.Equal(t, i3, i4)
}

func TestIntervalSlice_Sort(t *testing.T) {
	cases := []struct {
		islice       IntervalSlice
		isliceSorted IntervalSlice
	}{
		{
			IntervalSlice{
				Interval{Start: 1, Stop: 6},
				Interval{Start: 1, Stop: 5},
			},
			IntervalSlice{
				Interval{Start: 1, Stop: 5},
				Interval{Start: 1, Stop: 6},
			},
		},
		{
			IntervalSlice{
				Interval{Start: 1, Stop: 5},
				Interval{Start: 10, Stop: 15},
				Interval{Start: 5, Stop: 10},
			},
			IntervalSlice{
				Interval{Start: 1, Stop: 5},
				Interval{Start: 5, Stop: 10},
				Interval{Start: 10, Stop: 15},
			},
		},
		{
			// duplicate interval, not expected.
			IntervalSlice{
				Interval{Start: 1, Stop: 5},
				Interval{Start: 10, Stop: 15},
				Interval{Start: 10, Stop: 15},
				Interval{Start: 5, Stop: 10},
			},
			IntervalSlice{
				Interval{Start: 1, Stop: 5},
				Interval{Start: 5, Stop: 10},
				Interval{Start: 10, Stop: 15},
				Interval{Start: 10, Stop: 15},
			},
		},
		{
			// tag and start equal, which is not expected.
			IntervalSlice{
				Interval{Start: 1, Stop: 6},
				Interval{Start: 1, Stop: 5},
				Interval{Start: 1, Stop: 7},
				Interval{Start: 1, Stop: 8},
			},
			IntervalSlice{
				Interval{Start: 1, Stop: 5},
				Interval{Start: 1, Stop: 6},
				Interval{Start: 1, Stop: 7},
				Interval{Start: 1, Stop: 8},
			},
		},
	}

	for _, tc := range cases {
		require.NotEqual(t, tc.isliceSorted, tc.islice)
		tc.islice.Sort()
		require.Equal(t, tc.isliceSorted, tc.islice)
	}
}
