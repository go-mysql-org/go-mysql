package mysql

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestParseUUIDSet(t *testing.T) {
	cases := []struct {
		gtid      string // input
		uuidSet   string // result
		tsid      string // tsid of result
		intervals IntervalSlice
	}{
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2",
			IntervalSlice{
				Interval{1, 2},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2",
			IntervalSlice{
				Interval{1, 4},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5-8",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5-8",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2",
			IntervalSlice{
				Interval{1, 4},
				Interval{5, 9},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5:7-8:10",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5:7-8:10",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2",
			IntervalSlice{
				Interval{1, 4},
				Interval{5, 6},
				Interval{7, 9},
				Interval{10, 11},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:foobar:1-10",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:foobar:1-10",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:foobar",
			IntervalSlice{
				Interval{1, 11},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:barfoo:1-5:foobar:1",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3", // only the first set
			"40eb4bee-1972-11f1-acc9-324f96ede8f2",
			IntervalSlice{
				Interval{1, 4},
			},
		},
		{
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo:1-4:6-8",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo:1-4:6-8",
			"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo",
			IntervalSlice{
				Interval{Start: 1, Stop: 5},
				Interval{Start: 6, Stop: 9},
			},
		},
	}

	for _, tc := range cases {
		uuidset, err := ParseUUIDSet(tc.gtid)
		require.NoError(t, err, tc.gtid)
		require.Equal(t, tc.uuidSet, uuidset.String())
		require.Equal(t, tc.tsid, uuidset.TSID.String())
		require.Equal(t, tc.intervals.Normalize(), uuidset.Intervals, tc.gtid)
	}
}

func FuzzParseUUIDSet(f *testing.F) {
	cases := []string{
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:1",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5-8",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:5:7-8:10",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:foobar:1-10",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:barfoo:1-5:foobar:1",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:abcdefgh:1:abcdefghijklmnopqrstuvwxyz:1:abcdefghijklmnopqrstuvwxyz_____:1:abcdefghijklmnopqrstuvwxyz_____x:1:foobar:1-2:foobaz:1:x:1",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo:1-4",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo:1-4:6-8",
		"40eb4bee-1972-11f1-acc9-324f96ede8f2:bar:6-8:10:foo:1-4:6-8",
	}

	for _, tc := range cases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, input string) {
		_, _ = ParseUUIDSet(input)
	})
}

func TestParseUUIDSet_Invalid(t *testing.T) {
	cases := []struct {
		gtid string
	}{
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-3:abcdefghijklmnopqrstuvwxyz_____xy:1"}, // tag too long
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:Foo:1-3"},                                 // tag not normalized
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo-bar:1-3"},                             // tag contains invalid character
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:foobar"},                                  // tag without start/stop
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:10-5"},                                    // start > stop
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo:10-5"},                                // tag and start > stop
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:1-2-3"},                                   // too many parts after split on -
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo:1-2-3"},                               // tag and too many parts after split on -
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:"},                                        // missing interval
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2"},                                         // split on : doesn't give two parts
		{"40eb4bee-1972-11f1-acc9-324f96ede8f:1-2"},                                      // Not a valid UUID
		{"40eb4bee-1972-11f1-acc9-324f96ede8f2:foo:bar:1-2"},                             // Tag not followed by interval
	}

	for _, tc := range cases {
		_, err := ParseUUIDSet(tc.gtid)
		require.Error(t, err, tc.gtid)
	}
}

func TestNewUUIDSet(t *testing.T) {
	tsid := TSID{SID: uuid.New()}
	s := NewUUIDSet(tsid)
	require.Equal(t, &UUIDSet{TSID: tsid}, s)

	s2 := NewUUIDSet(tsid, Interval{Start: 1, Stop: 5})
	require.Equal(t,
		&UUIDSet{
			TSID: tsid,
			Intervals: IntervalSlice{
				Interval{Start: 1, Stop: 5},
			},
		},
		s2,
	)

	s3 := NewUUIDSet(tsid,
		Interval{Start: 1, Stop: 5},
		Interval{Start: 1, Stop: 10},
	)
	require.Equal(t,
		&UUIDSet{
			TSID: tsid,
			Intervals: IntervalSlice{
				Interval{Start: 1, Stop: 10},
			},
		},
		s3,
	)
}

func TestUUIDSet(t *testing.T) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.NoError(t, err)
	require.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", us.String())

	buf := us.Encode(GtidFormatClassic)
	err = us.Decode(buf, GtidFormatClassic)
	require.NoError(t, err)
}

func TestUUIDSet_Clone(t *testing.T) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.NoError(t, err)
	require.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", us.String())

	clone := us.Clone()
	require.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", clone.String())
}

func TestUUIDSet_Contain(t *testing.T) {
	us1, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.NoError(t, err)

	us2, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20940:1-2")
	require.NoError(t, err)

	us3, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:foo:1-2")
	require.NoError(t, err)

	require.True(t, us1.Contain(us1))
	require.False(t, us1.Contain(us2))
	require.False(t, us1.Contain(us3))
}

func TestUUIDSet_Decode(t *testing.T) {
	tsid := TSID{
		SID: uuid.UUID{13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252},
	}
	cases := []struct {
		format    GtidFormat
		input     []byte
		set       *UUIDSet
		expectErr bool
	}{
		{
			GtidFormatClassic,
			[]byte{
				13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
				1, 0, 0, 0, 0, 0, 0, 0, // interval
				1, 0, 0, 0, 0, 0, 0, 0, // start
				0xa, 0, 0, 0, 0, 0, 0, 0, // stop
			},
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}),
			false,
		},
		{
			GtidFormatClassic,
			[]byte{
				13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
				1, 0, 0, 0, 0, 0, 0, 0, // interval
				1, 0, 0, 0, 0, 0, 0, 0, // start
				0xa, 0, 0, 0, 0, 0, 0, 0, // stop
				0xff, // invalid
			},
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}),
			true,
		},
		{
			// Possibly invalid
			GtidFormatClassic,
			[]byte{
				13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
				0, 0, 0, 0, 0, 0, 0, 0, // interval
			},
			&UUIDSet{
				TSID:      tsid,
				Intervals: IntervalSlice{},
			},
			false,
		},
		{
			// Possibly invalid
			GtidFormatClassic,
			[]byte{
				13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
				1, 0, 0, 0, 0, 0, 0, 0, // interval
				// truncated
			},
			&UUIDSet{
				TSID:      tsid,
				Intervals: IntervalSlice{},
			},
			true,
		},
		{
			GtidFormatClassic,
			[]byte{
				13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
				2, 0, 0, 0, 0, 0, 0, 0, // intervals
				1, 0, 0, 0, 0, 0, 0, 0, // start
				0x0a, 0, 0, 0, 0, 0, 0, 0, // stop
				0x7b, 0, 0, 0, 0, 0, 0, 0, // start
				0x80, 0xc3, 0xc9, 0x1, 0, 0, 0, 0, // stop
			},
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}, Interval{Start: 123, Stop: 30000000}),
			false,
		},
		{
			// 071a4ecc-1bf9-11f1-a838-e6dd1807d029:mytagabcdef:1-2
			GtidFormatTagged,
			[]byte{
				0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
				0x16,                                                             // tag length (needs >>1)
				0x6d, 0x79, 0x74, 0x61, 0x67, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, // tag
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
				0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
			},
			NewUUIDSet(
				TSID{
					SID: uuid.UUID{0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29},
					Tag: "mytagabcdef",
				},
				Interval{Start: 1, Stop: 3},
			),
			false,
		},
		{
			// 071a4ecc-1bf9-11f1-a838-e6dd1807d029:1-2:mytagabcdef:1
			GtidFormatTagged,
			[]byte{
				0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
				0x0,                                    // tag length (no tag)
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
				0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
				0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
				0x16,                                                             // tag length (needs >>1)
				0x6d, 0x79, 0x74, 0x61, 0x67, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, // tag
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
				0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
			},
			NewUUIDSet(
				TSID{SID: uuid.UUID{0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29}},
				Interval{Start: 1, Stop: 3},
			),
			true,
		},
	}

	for i, tc := range cases {
		rs := UUIDSet{}
		err := rs.Decode(tc.input, tc.format)
		if tc.expectErr {
			require.Error(t, err, "case %d", i)
		} else {
			if err != nil {
				t.Logf("\nExpected UUIDSet:\n%#v\nGot UUIDSet:\n%#v", tc.set, &rs)
			}
			require.NoError(t, err, "case %d", i)
			require.Equal(t, tc.set, &rs, "case %d", i)
		}
	}
}

func FuzzUUIDSet_Decode_classic(f *testing.F) {
	cases := [][]byte{
		{
			13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
			1, 0, 0, 0, 0, 0, 0, 0, // interval
			1, 0, 0, 0, 0, 0, 0, 0, // start
			0xa, 0, 0, 0, 0, 0, 0, 0, // stop
		},
		{
			13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
			0, 0, 0, 0, 0, 0, 0, 0, // interval
		},
		{
			13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
			2, 0, 0, 0, 0, 0, 0, 0, // intervals
			1, 0, 0, 0, 0, 0, 0, 0, // start
			0x0a, 0, 0, 0, 0, 0, 0, 0, // stop
			0x7b, 0, 0, 0, 0, 0, 0, 0, // start
			0x80, 0xc3, 0xc9, 0x1, 0, 0, 0, 0, // stop
		},
	}

	for _, tc := range cases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		rs := UUIDSet{}
		_ = rs.Decode(input, GtidFormatClassic)
	})
}

func FuzzUUIDSet_Decode_tagged(f *testing.F) {
	cases := [][]byte{
		{
			0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
			0x16,                                                             // tag length (needs >>1)
			0x6d, 0x79, 0x74, 0x61, 0x67, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, // tag
			0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
			0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
			0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
		},
		{
			0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
			0x0,                                    // tag length (no tag)
			0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
			0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
			0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
			0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
			0x16,                                                             // tag length (needs >>1)
			0x6d, 0x79, 0x74, 0x61, 0x67, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, // tag
			0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
			0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
			0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
		},
	}

	for _, tc := range cases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		rs := UUIDSet{}
		_ = rs.Decode(input, GtidFormatTagged)
	})
}

func TestUUIDSet_Encode(t *testing.T) {
	tsid := TSID{
		SID: uuid.UUID{13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252},
	}
	cases := []struct {
		set    *UUIDSet
		format GtidFormat
		result []byte
	}{
		{
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}),
			GtidFormatClassic,
			[]byte{
				13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
				1, 0, 0, 0, 0, 0, 0, 0, // interval
				1, 0, 0, 0, 0, 0, 0, 0, // start
				0xa, 0, 0, 0, 0, 0, 0, 0, // stop
			},
		},
		{
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}, Interval{Start: 123, Stop: 30000000}),
			GtidFormatClassic,
			[]byte{
				13, 253, 150, 255, 112, 142, 65, 232, 128, 185, 191, 63, 181, 206, 227, 252, // uuid
				2, 0, 0, 0, 0, 0, 0, 0, // intervals
				1, 0, 0, 0, 0, 0, 0, 0, // start
				0x0a, 0, 0, 0, 0, 0, 0, 0, // stop
				0x7b, 0, 0, 0, 0, 0, 0, 0, // start
				0x80, 0xc3, 0xc9, 0x1, 0, 0, 0, 0, // stop
			},
		},
		{
			// 071a4ecc-1bf9-11f1-a838-e6dd1807d029::mytagabcdef:1-2
			NewUUIDSet(
				TSID{
					SID: uuid.UUID{0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29},
					Tag: "mytagabcdef",
				},
				Interval{Start: 1, Stop: 3},
			),
			GtidFormatTagged,
			[]byte{
				0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
				0x16,                                                             // tag length (needs >>1)
				0x6d, 0x79, 0x74, 0x61, 0x67, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, // tag
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
				0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
			},
		},
	}

	for i, tc := range cases {
		d := tc.set.Encode(tc.format)
		require.Equal(t, tc.result, d, "case %d (format %s)", i, tc.format.String())
	}
}

func TestUUIDSet_MinusInterval(t *testing.T) {
	tsid := TSID{
		SID: uuid.New(),
	}
	cases := []struct {
		set    *UUIDSet
		in     IntervalSlice
		result *UUIDSet
	}{
		{
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}),
			IntervalSlice{
				Interval{Start: 8, Stop: 10},
			},
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 8}),
		},
		{
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}),
			IntervalSlice{
				Interval{Start: 20, Stop: 21},
			},
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}),
		},
		{
			NewUUIDSet(tsid, Interval{Start: 1, Stop: 10}),
			IntervalSlice{
				Interval{Start: 5, Stop: 8},
			},
			NewUUIDSet(tsid,
				Interval{Start: 1, Stop: 5},
				Interval{Start: 8, Stop: 10},
			),
		},
	}
	for _, tc := range cases {
		tc.set.MinusInterval(tc.in)
		require.Equal(t, tc.result, tc.set)
	}
}
