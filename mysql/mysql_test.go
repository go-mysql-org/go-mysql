package mysql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	_ "github.com/go-mysql-org/go-mysql/test_util" // Will register common flags
)

func TestMysqlGTIDInterval(t *testing.T) {
	i, err := parseInterval("1-2")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 3}, i)

	i, err = parseInterval("1")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 2}, i)

	i, err = parseInterval("1-1")
	require.NoError(t, err)
	require.Equal(t, Interval{1, 2}, i)
}

func TestMysqlGTIDIntervalSlice(t *testing.T) {
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

func TestMysqlGTIDInsertInterval(t *testing.T) {
	i := IntervalSlice{Interval{100, 200}}
	i.InsertInterval(Interval{300, 400})
	require.Equal(t, IntervalSlice{Interval{100, 200}, Interval{300, 400}}, i)

	i.InsertInterval(Interval{50, 70})
	require.Equal(t, IntervalSlice{Interval{50, 70}, Interval{100, 200}, Interval{300, 400}}, i)

	i.InsertInterval(Interval{101, 201})
	require.Equal(t, IntervalSlice{Interval{50, 70}, Interval{100, 201}, Interval{300, 400}}, i)

	i.InsertInterval(Interval{99, 202})
	require.Equal(t, IntervalSlice{Interval{50, 70}, Interval{99, 202}, Interval{300, 400}}, i)

	i.InsertInterval(Interval{102, 302})
	require.Equal(t, IntervalSlice{Interval{50, 70}, Interval{99, 400}}, i)

	i.InsertInterval(Interval{500, 600})
	require.Equal(t, IntervalSlice{Interval{50, 70}, Interval{99, 400}, Interval{500, 600}}, i)

	i.InsertInterval(Interval{50, 100})
	require.Equal(t, IntervalSlice{Interval{50, 400}, Interval{500, 600}}, i)

	i.InsertInterval(Interval{900, 1000})
	require.Equal(t, IntervalSlice{Interval{50, 400}, Interval{500, 600}, Interval{900, 1000}}, i)

	i.InsertInterval(Interval{1010, 1020})
	require.Equal(t, IntervalSlice{Interval{50, 400}, Interval{500, 600}, Interval{900, 1000}, Interval{1010, 1020}}, i)

	i.InsertInterval(Interval{49, 1000})
	require.Equal(t, IntervalSlice{Interval{49, 1000}, Interval{1010, 1020}}, i)

	i.InsertInterval(Interval{1, 1012})
	require.Equal(t, IntervalSlice{Interval{1, 1020}}, i)
}

func TestMysqlGTIDCodec(t *testing.T) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.NoError(t, err)

	require.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", us.String())

	buf := us.Encode()
	err = us.Decode(buf)
	require.NoError(t, err)

	gs, err := ParseMysqlGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2,de278ad0-2106-11e4-9f8e-6edd0ca20948:1-2")
	require.NoError(t, err)

	buf = gs.Encode()
	o, err := DecodeMysqlGTIDSet(buf)
	require.NoError(t, err)
	require.Equal(t, gs, o)
}

func TestMysqlUpdate(t *testing.T) {
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

func TestMysqlAddGTID(t *testing.T) {
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

func TestMysqlGTIDContain(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	require.NoError(t, err)

	g2, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	require.NoError(t, err)

	require.True(t, g2.Contain(g1))
	require.False(t, g1.Contain(g2))
}

func TestMysqlGTIDAdd(t *testing.T) {
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

func TestMysqlGTIDMinus(t *testing.T) {
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

func TestMysqlParseBinaryInt8(t *testing.T) {
	i8 := ParseBinaryInt8([]byte{128})
	require.Equal(t, int8(-128), i8)
}

func TestMysqlParseBinaryUint8(t *testing.T) {
	u8 := ParseBinaryUint8([]byte{128})
	require.Equal(t, uint8(128), u8)
}

func TestMysqlParseBinaryInt16(t *testing.T) {
	i16 := ParseBinaryInt16([]byte{1, 128})
	require.Equal(t, int16(-128*256+1), i16)
}

func TestMysqlParseBinaryUint16(t *testing.T) {
	u16 := ParseBinaryUint16([]byte{1, 128})
	require.Equal(t, uint16(128*256+1), u16)
}

func TestMysqlParseBinaryInt24(t *testing.T) {
	i32 := ParseBinaryInt24([]byte{1, 2, 128})
	require.Equal(t, int32(-128*65536+2*256+1), i32)
}

func TestMysqlParseBinaryUint24(t *testing.T) {
	u32 := ParseBinaryUint24([]byte{1, 2, 128})
	require.Equal(t, uint32(128*65536+2*256+1), u32)
}

func TestMysqlParseBinaryInt32(t *testing.T) {
	i32 := ParseBinaryInt32([]byte{1, 2, 3, 128})
	require.Equal(t, int32(-128*16777216+3*65536+2*256+1), i32)
}

func TestMysqlParseBinaryUint32(t *testing.T) {
	u32 := ParseBinaryUint32([]byte{1, 2, 3, 128})
	require.Equal(t, uint32(128*16777216+3*65536+2*256+1), u32)
}

func TestMysqlParseBinaryInt64(t *testing.T) {
	i64 := ParseBinaryInt64([]byte{1, 2, 3, 4, 5, 6, 7, 128})
	require.Equal(t, -128*int64(72057594037927936)+7*int64(281474976710656)+6*int64(1099511627776)+5*int64(4294967296)+4*16777216+3*65536+2*256+1, i64)
}

func TestMysqlParseBinaryUint64(t *testing.T) {
	u64 := ParseBinaryUint64([]byte{1, 2, 3, 4, 5, 6, 7, 128})
	require.Equal(t, 128*uint64(72057594037927936)+7*uint64(281474976710656)+6*uint64(1099511627776)+5*uint64(4294967296)+4*16777216+3*65536+2*256+1, u64)
}

func TestErrorCode(t *testing.T) {
	tbls := []struct {
		msg  string
		code int
	}{
		{"ERROR 1094 (HY000): Unknown thread id: 1094", 1094},
		{"error string", 0},
		{"abcdefg", 0},
		{"123455 ks094", 0},
		{"ERROR 1046 (3D000): Unknown error 1046", 1046},
	}
	for _, v := range tbls {
		require.Equal(t, v.code, ErrorCode(v.msg))
	}
}

func TestMysqlNullDecode(t *testing.T) {
	_, isNull, n := LengthEncodedInt([]byte{0xfb})

	require.True(t, isNull)
	require.Equal(t, 1, n)
}

func TestMysqlUUIDClone(t *testing.T) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	require.NoError(t, err)
	require.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", us.String())

	clone := us.Clone()
	require.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", clone.String())
}

func TestMysqlEmptyDecode(t *testing.T) {
	_, isNull, n := LengthEncodedInt(nil)
	require.True(t, isNull)
	require.Equal(t, 0, n)
}

func mysqlGTIDfromString(t *testing.T, gtidStr string) MysqlGTIDSet {
	gtid, err := ParseMysqlGTIDSet(gtidStr)
	require.NoError(t, err)

	return *gtid.(*MysqlGTIDSet)
}
