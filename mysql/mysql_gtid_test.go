package mysql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mysqlGTIDfromString(t *testing.T, gtidStr string) MysqlGTIDSet {
	gtid, err := ParseMysqlGTIDSet(gtidStr)
	require.NoError(t, err)

	return *gtid.(*MysqlGTIDSet)
}

func TestDecodeSid(t *testing.T) {
	testcases := []struct {
		input      []byte
		gtidFormat GtidFormat
		uuidCount  uint64
	}{
		{[]byte{1, 2, 0, 0, 0, 0, 0, 1}, GtidFormatTagged, 2},
		{[]byte{1, 1, 0, 0, 0, 0, 0, 1}, GtidFormatTagged, 1},
		{[]byte{1, 0, 0, 0, 0, 0, 0, 1}, GtidFormatTagged, 0},
		{[]byte{1, 0, 0, 0, 0, 0, 0, 0}, GtidFormatClassic, 1},
	}

	for _, tc := range testcases {
		format, uuidCount := DecodeSid(tc.input)
		assert.Equal(t, tc.gtidFormat, format)
		assert.Equal(t, tc.uuidCount, uuidCount)
	}
}

func FuzzDecodeSid(f *testing.F) {
	testcases := [][]byte{
		{1, 2, 0, 0, 0, 0, 0, 1},
		{1, 1, 0, 0, 0, 0, 0, 1},
		{1, 0, 0, 0, 0, 0, 0, 1},
		{1, 0, 0, 0, 0, 0, 0, 0},
	}

	for _, tc := range testcases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		fmt, sidnr := DecodeSid(input)
		enc := encodeSid(fmt, sidnr)
		if len(input) >= 8 {
			if fmt == GtidFormatTagged {
				// If the first byte is always encoded as 0x1
				require.Equal(t, input[1:7], enc[1:7])
			} else {
				require.Equal(t, input[0:7], enc[0:7])
			}
		}
	})
}

func TestGtidFormat_String(t *testing.T) {
	require.Equal(t, GtidFormat(3).String(), "GtidFormat{3}")
}

func TestParseMysqlGTIDSet(t *testing.T) {
	_, err := ParseMysqlGTIDSet("")
	require.NoError(t, err)

	_, err = ParseMysqlGTIDSet(",")
	require.Error(t, err)

	_, err = ParseMysqlGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2:foo:bar:1-2")
	require.Error(t, err)
}

func TestDecodeMysqlGTIDSet(t *testing.T) {
	_, err := DecodeMysqlGTIDSet([]byte{0, 0, 0, 0, 0, 0, 0}) // 7 byte
	require.Error(t, err)

	// Not sure if this should be legal
	_, err = DecodeMysqlGTIDSet([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	require.NoError(t, err)

	_, err = DecodeMysqlGTIDSet([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	require.Error(t, err)
}

func TestMysqlGTIDSet(t *testing.T) {
	gs, err := ParseMysqlGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2,de278ad0-2106-11e4-9f8e-6edd0ca20948:1-2")
	require.NoError(t, err)

	buf := gs.Encode()
	o, err := DecodeMysqlGTIDSet(buf)
	require.NoError(t, err)
	require.Equal(t, gs, o)

	ts, err := ParseMysqlGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2:mytag:1-10:12-14:16:18-20,de278ad0-2106-11e4-9f8e-6edd0ca20948:1-2")
	require.NoError(t, err)
	buf = ts.Encode()
	o, err = DecodeMysqlGTIDSet(buf)
	require.NoError(t, err)
	require.Equal(t, ts, o)

	setstr := "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2:mytag:1-10:12-14:16:18-20"
	ts2, err := ParseMysqlGTIDSet(setstr)
	require.NoError(t, err)
	require.Equal(t, setstr, ts2.String())
	buf = ts2.Encode()
	// From Wireshark
	// mysqlbinlog --read-from-remote-source=BINLOG-DUMP-GTIDS -h 127.0.0.1 -u root --stop-never --exclude-gtids 'de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2:mytag:1-10:12-14:16:18-20' --ssl-mode=disabled mysql-bin.000001 --connection-server-id=876
	// Then for the Send Binlog GTID packet:
	// Select mysql.binlog.gtid_data and then use the "Copy...as Go literal"
	// Unmodified, except for splitting it into multiple lines and adding annotations.
	dat := []byte{
		0x1, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // format marker: tagged, sidnr: 0x2
		0xde, 0x27, 0x8a, 0xd0, 0x21, 0x6, 0x11, 0xe4, 0x9f, 0x8e, 0x6e, 0xdd, 0xc, 0xa2, 0x9, 0x47, // uuid
		0x0,                                    // tag length: 0
		0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals: 1
		0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start: 1
		0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop: 3
		0xde, 0x27, 0x8a, 0xd0, 0x21, 0x6, 0x11, 0xe4, 0x9f, 0x8e, 0x6e, 0xdd, 0xc, 0xa2, 0x9, 0x47, // uuid
		0xa,                          // tag length 0xa>>1 = 5
		0x6d, 0x79, 0x74, 0x61, 0x67, // tag
		0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals: 4
		0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start:  1
		0xb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop: 11
		0xc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start: 12
		0xf, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop: 15
		0x10, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start: 16
		0x11, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop: 17
		0x12, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start: 18
		0x15, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop: 21
	}
	require.Equal(t, dat, buf)
	o, err = DecodeMysqlGTIDSet(buf)
	require.NoError(t, err)
	require.Equal(t, ts2, o)
}

func TestMysqlGTIDSet_Add(t *testing.T) {
	testCases := []struct {
		left, right, expected string
	}{
		// simple cases works:
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:23", "3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23:28-57"},
		// summ is associative operation
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23:28-57"},
		// merge intervals:
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:23-27", "3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23-57"},
	}

	for _, tc := range testCases {
		m1 := mysqlGTIDfromString(t, tc.left)
		m2 := mysqlGTIDfromString(t, tc.right)
		err := m1.Add(m2)
		require.NoError(t, err)
		one := fmt.Sprintf("%s + %s = %s", tc.left, tc.right, strings.ToUpper(m1.String()))
		other := fmt.Sprintf("%s + %s = %s", tc.left, tc.right, tc.expected)
		require.Equal(t, other, one)
	}
}

func TestMysqlGTIDSet_AddGTID(t *testing.T) {
	g, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	g1 := g.(*MysqlGTIDSet)

	u, err := uuid.Parse("3E11FA47-71CA-11E1-9E33-C80AA9429562")
	require.NoError(t, err)

	g1.AddGTID(u, 58)
	require.Equal(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58", strings.ToUpper(g1.String()))

	g1.AddGTID(u, 60)
	require.Equal(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58:60", strings.ToUpper(g1.String()))

	g1.AddGTID(u, 59)
	require.Equal(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:21-60", strings.ToUpper(g1.String()))

	u2, err := uuid.Parse("519CE70F-A893-11E9-A95A-B32DC65A7026")
	require.NoError(t, err)
	g1.AddGTID(u2, 58)
	g2, err := ParseMysqlGTIDSet(`
	3E11FA47-71CA-11E1-9E33-C80AA9429562:21-60,
	519CE70F-A893-11E9-A95A-B32DC65A7026:58
`)
	require.NoError(t, err)
	require.True(t, g2.Equal(g1))
}

func TestMysqlGTIDSet_AddSet(t *testing.T) {
	g, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	if gset, ok := g.(*MysqlGTIDSet); ok {
		gset.AddSet(nil)
	}
}

func TestMysqlGTIDSet_Clone(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	require.NoError(t, err)

	g2 := g1.Clone()
	require.Equal(t, g1, g2)
}

func TestMysqlGTIDSet_Contain(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	require.NoError(t, err)

	g2, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	require.True(t, g2.Contain(g1))
	require.False(t, g1.Contain(g2))

	g3, err := ParseGTIDSet(MariaDBFlavor, "0-1-2")
	require.NoError(t, err)
	require.False(t, g1.Contain(g3))
}

func TestMysqlGTIDSet_Encode(t *testing.T) {
	cases := []struct {
		set    MysqlGTIDSet
		result []byte
	}{
		{
			MysqlGTIDSet{
				Sets: map[TSID]*UUIDSet{
					{
						SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"),
					}: NewUUIDSet(
						TSID{
							SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"),
						},
						Interval{Start: 1, Stop: 3},
					),
				},
			},
			[]byte{
				// Classic format, not tagged
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // nr of sids + format tag
				0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
				0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
			},
		},
		{
			MysqlGTIDSet{
				Sets: map[TSID]*UUIDSet{
					{
						SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"),
						Tag: "mytagabcdef",
					}: NewUUIDSet(
						TSID{SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"), Tag: "mytagabcdef"},
						Interval{Start: 1, Stop: 2},
					),
				},
			},
			[]byte{
				// Tagged format
				0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // nr of sids + format tag
				0x7, 0x1a, 0x4e, 0xcc, 0x1b, 0xf9, 0x11, 0xf1, 0xa8, 0x38, 0xe6, 0xdd, 0x18, 0x7, 0xd0, 0x29, // uuid
				0x16,                                                             // tag length (needs >>1)
				0x6d, 0x79, 0x74, 0x61, 0x67, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, // tag
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // intervals
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // start
				0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // stop
			},
		},
		{
			MysqlGTIDSet{
				Sets: map[TSID]*UUIDSet{
					{
						SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"),
					}: NewUUIDSet(
						TSID{SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029")},
						Interval{Start: 1, Stop: 3},
					),
					{
						SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"),
						Tag: "mytagabcdef",
					}: NewUUIDSet(
						TSID{SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"), Tag: "mytagabcdef"},
						Interval{Start: 1, Stop: 2},
					),
				},
			},
			[]byte{
				// Tagged format
				0x1, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // nr of sids + format tag
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
		},
		{
			// Same as the one above, but with the Sets in a different order to test sorting.
			MysqlGTIDSet{
				Sets: map[TSID]*UUIDSet{
					{
						SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"),
						Tag: "mytagabcdef",
					}: NewUUIDSet(
						TSID{SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"), Tag: "mytagabcdef"},
						Interval{Start: 1, Stop: 2},
					),
					{
						SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029"),
					}: NewUUIDSet(
						TSID{SID: uuid.MustParse("071a4ecc-1bf9-11f1-a838-e6dd1807d029")},
						Interval{Start: 1, Stop: 3},
					),
				},
			},
			[]byte{
				// Tagged format
				0x1, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // nr of sids + format tag
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
		},
	}

	for i, tc := range cases {
		require.Equal(t, tc.result, tc.set.Encode(), "case %d", i)
	}
}

func TestMysqlGTIDSet_Equal(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	require.NoError(t, err)

	g2, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	require.False(t, g2.Equal(g1))
	require.False(t, g1.Equal(g2))
	require.True(t, g1.Equal(g1))

	g3, err := ParseGTIDSet(MariaDBFlavor, "0-1-2")
	require.NoError(t, err)
	require.False(t, g1.Equal(g3))

	g4, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57,3E11FA47-71CA-11E1-9E33-C80AA9429563:11-17")
	require.NoError(t, err)
	require.False(t, g1.Equal(g4))
}

func TestMysqlGTIDSet_IsEmpty(t *testing.T) {
	emptyGTIDSet := new(MysqlGTIDSet)
	emptyGTIDSet.Sets = make(map[TSID]*UUIDSet)
	require.True(t, emptyGTIDSet.IsEmpty())

	nonEmptyGTIDSet := mysqlGTIDfromString(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.False(t, nonEmptyGTIDSet.IsEmpty())
}

func TestMysqlGTIDSet_Minus(t *testing.T) {
	testCases := []struct {
		left, right, expected string
	}{
		// Minuses that doesn't affect original value:
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:23", "3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23"},
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23", "3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57"},
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:23", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-22:24-57", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23"},
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:23", "ABCDEF12-1234-5678-9012-345678901234:1-1000", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23"},
		// Minuses that change original value:
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:20-57:60-90", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23", "3E11FA47-71CA-11E1-9E33-C80AA9429562:20-22:24-57:60-90"},
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:20-57:60-90", "3E11FA47-71CA-11E1-9E33-C80AA9429562:22-70", "3E11FA47-71CA-11E1-9E33-C80AA9429562:20-21:71-90"},
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57", "3E11FA47-71CA-11E1-9E33-C80AA9429562:28-57", ""},
		{"3E11FA47-71CA-11E1-9E33-C80AA9429562:20-21", "3E11FA47-71CA-11E1-9E33-C80AA9429562:21", "3E11FA47-71CA-11E1-9E33-C80AA9429562:20"},
		{"582A11ED-786C-11EC-ACCC-E0356662B76E:1-209692", "582A11ED-786C-11EC-ACCC-E0356662B76E:1-146519", "582A11ED-786C-11EC-ACCC-E0356662B76E:146520-209692"},
		{"582A11ED-786C-11EC-ACCC-E0356662B76E:1-209692", "582A11ED-786C-11EC-ACCC-E0356662B76E:2-146519", "582A11ED-786C-11EC-ACCC-E0356662B76E:1:146520-209692"},
	}

	for _, tc := range testCases {
		m1 := mysqlGTIDfromString(t, tc.left)
		m2 := mysqlGTIDfromString(t, tc.right)
		err := m1.Minus(m2)
		require.NoError(t, err)
		one := fmt.Sprintf("%s - %s = %s", tc.left, tc.right, strings.ToUpper(m1.String()))
		other := fmt.Sprintf("%s - %s = %s", tc.left, tc.right, tc.expected)
		require.Equal(t, other, one)
	}
}

func TestMysqlGTIDSet_Update(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	err = g1.Update("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58")
	require.NoError(t, err)

	require.Equal(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58", strings.ToUpper(g1.String()))

	g1, err = ParseMysqlGTIDSet(`
		519CE70F-A893-11E9-A95A-B32DC65A7026:1-1154661,
		5C9CA52B-9F11-11E9-8EAF-3381EC1CC790:1-244,
		802D69FD-A3B6-11E9-B1EA-50BAB55BA838:1-1221371,
		F2B50559-A891-11E9-B646-884FF0CA2043:1-479261
	`)
	require.NoError(t, err)

	err = g1.Update(`
		802D69FD-A3B6-11E9-B1EA-50BAB55BA838:1221110-1221371,
		F2B50559-A891-11E9-B646-884FF0CA2043:478509-479266
	`)
	require.NoError(t, err)

	g2, err := ParseMysqlGTIDSet(`
		519CE70F-A893-11E9-A95A-B32DC65A7026:1-1154661,
		5C9CA52B-9F11-11E9-8EAF-3381EC1CC790:1-244,
		802D69FD-A3B6-11E9-B1EA-50BAB55BA838:1-1221371,
		F2B50559-A891-11E9-B646-884FF0CA2043:1-479266
	`)
	require.NoError(t, err)
	require.True(t, g1.Equal(g2))
}

func TestMysqlGTIDSet_String(t *testing.T) {
	cases := []struct {
		input  string
		output string
	}{
		{
			"3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57",
			"3e11fa47-71ca-11e1-9e33-c80aa9429562:21-57",
		},
		{
			" 3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57:60-64",
			"3e11fa47-71ca-11e1-9e33-c80aa9429562:21-57:60-64",
		},
		{
			"3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57:foo:5 ",
			"3e11fa47-71ca-11e1-9e33-c80aa9429562:21-57:foo:5",
		},
		{
			"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10,3E11FA47-71CA-11E1-9E33-C80AA9429563:1-20",
			"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10,3e11fa47-71ca-11e1-9e33-c80aa9429563:1-20",
		},
	}

	for _, tc := range cases {
		g, err := ParseMysqlGTIDSet(tc.input)
		require.NoError(t, err)

		require.Equal(t, tc.output, g.String())
	}
}
