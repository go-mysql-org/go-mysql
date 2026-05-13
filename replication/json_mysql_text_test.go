package replication

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestFormatMySQLDouble(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		// Whole-number doubles must keep the trailing zero so MySQL re-stores
		// them as JSON DOUBLE, not JSON INTEGER. This is the bug
		// UseFloatWithTrailingZero already addresses for the non-MySQL-text
		// path; the renderer must match that behaviour.
		{1.0, "1.0"},
		{0.0, "0.0"},
		{-3.0, "-3.0"},
		{1e10, "10000000000.0"},
		// Non-integer values: shortest round-trippable form.
		{3.14, "3.14"},
		{-2.5, "-2.5"},
		{0.1, "0.1"},
		{1.5e-5, "1.5e-05"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, formatMySQLDouble(c.in), "in=%v", c.in)
	}
	// NaN/Inf should not corrupt the surrounding document.
	require.Equal(t, "null", formatMySQLDouble(math.NaN()))
	require.Equal(t, "null", formatMySQLDouble(math.Inf(1)))
	require.Equal(t, "null", formatMySQLDouble(math.Inf(-1)))
}

func TestWriteJSONString(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{`hello`, `hello`},
		{`a"b`, `a\"b`},
		{`a\b`, `a\\b`},
		{"line1\nline2", `line1\nline2`},
		{"tab\there", `tab\there`},
		{"ctrl\x01char", `ctrl\u0001char`},
		{"unicode: é 漢", "unicode: é 漢"},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		writeJSONString(&buf, []byte(c.in))
		require.Equal(t, c.want, buf.String(), "in=%q", c.in)
	}
}

// TestRenderJSONAsMySQLText_Double exercises the renderer directly on a
// hand-constructed JSONB_DOUBLE byte stream (the simplest JSONB layout:
// a single scalar). It confirms the renderer emits MySQL-style "1.0" for
// a whole-number double instead of "1" (which the legacy decoder would
// produce because float64(1.0) marshals to "1" via json.Marshal).
//
// JSONB_OBJECT / JSONB_ARRAY / JSONB_OPAQUE layouts (offset tables,
// inline-vs-pointer values, NEWDECIMAL/DATETIME payloads) are covered by
// the fixture tests below. Full end-to-end coverage against a live MySQL
// binlog stream lives in github.com/block/spirit's pkg/repl tests.
func TestRenderJSONAsMySQLText_Double(t *testing.T) {
	// JSONB top-level value: a single type byte followed by 8 bytes of
	// little-endian IEEE 754 representing 1.0.
	data := make([]byte, 1+8)
	data[0] = JSONB_DOUBLE
	binary.LittleEndian.PutUint64(data[1:], math.Float64bits(1.0))

	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "1.0", string(out))
}

// TestRenderJSONAsMySQLText_Int verifies that JSONB integers stay
// integers (no spurious ".0" decoration).
func TestRenderJSONAsMySQLText_Int(t *testing.T) {
	data := make([]byte, 1+2)
	data[0] = JSONB_INT16
	binary.LittleEndian.PutUint16(data[1:], uint16(int16(42)))

	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "42", string(out))
}

// TestRenderJSONAsMySQLText_Literal exercises the LITERAL path.
func TestRenderJSONAsMySQLText_Literal(t *testing.T) {
	cases := []struct {
		lit  byte
		want string
	}{
		{JSONB_NULL_LITERAL, "null"},
		{JSONB_TRUE_LITERAL, "true"},
		{JSONB_FALSE_LITERAL, "false"},
	}
	for _, c := range cases {
		data := []byte{JSONB_LITERAL, c.lit}
		out, err := renderJSONAsMySQLText(data, false)
		require.NoError(t, err)
		require.Equal(t, c.want, string(out))
	}
}

// TestRenderJSONAsMySQLText_EmptyObject and _EmptyArray check the
// degenerate header-only cases.
func TestRenderJSONAsMySQLText_EmptyObject(t *testing.T) {
	// SMALL_OBJECT body: count=0 (LE u16), size=4 (LE u16) -- just the header.
	data := []byte{JSONB_SMALL_OBJECT, 0x00, 0x00, 0x04, 0x00}
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "{}", string(out))
}

func TestRenderJSONAsMySQLText_EmptyArray(t *testing.T) {
	data := []byte{JSONB_SMALL_ARRAY, 0x00, 0x00, 0x04, 0x00}
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "[]", string(out))
}

// TestRenderJSONAsMySQLText_Object exercises the SMALL_OBJECT offset table
// with one inline value (INT16) and one pointer value (STRING). This is
// the main path that risks getting offset arithmetic wrong.
func TestRenderJSONAsMySQLText_Object(t *testing.T) {
	// Layout for {"a": 1, "b": "x"} as JSONB SMALL_OBJECT body:
	//   0-1   count = 2                       (LE u16)
	//   2-3   total body size = 22            (LE u16)
	//   4-7   key entry 0: offset=18, len=1   ("a")
	//   8-11  key entry 1: offset=19, len=1   ("b")
	//   12-14 value entry 0: INT16 inline, 1
	//   15-17 value entry 1: STRING, offset=20
	//   18    'a'
	//   19    'b'
	//   20    varlen prefix = 1
	//   21    'x'
	body := []byte{
		0x02, 0x00, // count = 2
		0x16, 0x00, // size = 22
		0x12, 0x00, 0x01, 0x00, // key entry 0
		0x13, 0x00, 0x01, 0x00, // key entry 1
		JSONB_INT16, 0x01, 0x00, // value 0: INT16 inline = 1
		JSONB_STRING, 0x14, 0x00, // value 1: STRING at offset 20
		'a',
		'b',
		0x01, 'x',
	}
	data := append([]byte{JSONB_SMALL_OBJECT}, body...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `{"a": 1, "b": "x"}`, string(out))
}

// TestRenderJSONAsMySQLText_Array exercises SMALL_ARRAY with a mix of
// inline scalars (INT16, LITERAL) and a pointer value (STRING).
func TestRenderJSONAsMySQLText_Array(t *testing.T) {
	// Layout for [1, "x", true]:
	//   0-1  count = 3
	//   2-3  size = 15
	//   4-6  value 0: INT16 inline = 1
	//   7-9  value 1: STRING at offset 13
	//   10-12 value 2: LITERAL inline = true
	//   13   varlen = 1
	//   14   'x'
	body := []byte{
		0x03, 0x00, // count = 3
		0x0F, 0x00, // size = 15
		JSONB_INT16, 0x01, 0x00, // value 0: INT16 inline = 1
		JSONB_STRING, 0x0D, 0x00, // value 1: STRING at offset 13
		JSONB_LITERAL, JSONB_TRUE_LITERAL, 0x00, // value 2: LITERAL inline true
		0x01, 'x',
	}
	data := append([]byte{JSONB_SMALL_ARRAY}, body...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `[1, "x", true]`, string(out))
}

// TestRenderJSONAsMySQLText_OpaqueDecimal exercises the NEWDECIMAL path
// inside JSONB_OPAQUE. This is the value class where the legacy decoder
// loses the JSON DECIMAL type (it becomes a quoted JSON STRING) and the
// renderer must keep the value unquoted.
func TestRenderJSONAsMySQLText_OpaqueDecimal(t *testing.T) {
	// For precision=2, scale=1 the MySQL DECIMAL binary form needs 1 byte
	// for the integer digit and 1 byte for the fractional digit. The sign
	// is encoded in the high bit of the first byte (set => positive).
	//   0x81 = positive '1'
	//   0x00 = '0' fractional
	data := []byte{
		JSONB_OPAQUE,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		0x04,       // payload varlen = 4 bytes
		0x02, 0x01, // precision=2, scale=1
		0x81, 0x00, // decimal bytes: 1.0
	}
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	// MySQL renders JSON DECIMAL values unquoted -- this is the whole
	// point of the renderer for round-tripping back into JSON.
	require.Equal(t, "1.0", string(out))
}

func encodeJSONDateTimePayload(t *testing.T, year, month, day, hour, minute, second, frac int64) []byte {
	t.Helper()
	ym := year*13 + month
	ymd := (ym << 5) | day
	hms := (hour << 12) | (minute << 6) | second
	intPart := (ymd << 17) | hms
	v := (intPart << 24) | frac
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, uint64(v))
	return payload
}

func encodeJSONTimePayload(t *testing.T, hour, minute, second, frac int64) []byte {
	t.Helper()
	intPart := (hour << 12) | (minute << 6) | second
	v := (intPart << 24) | frac
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, uint64(v))
	return payload
}

// TestRenderJSONAsMySQLText_OpaqueDateTime exercises the DATETIME path.
// MySQL emits these quoted with microsecond resolution.
func TestRenderJSONAsMySQLText_OpaqueDateTime(t *testing.T) {
	payload := encodeJSONDateTimePayload(t, 2024, 1, 15, 10, 30, 45, 123456)
	data := append([]byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_DATETIME, 0x08}, payload...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `"2024-01-15 10:30:45.123456"`, string(out))
}

// TestRenderJSONAsMySQLText_OpaqueDate exercises the DATE path. The
// renderer is more correct here than the legacy decoder, which always
// formats DATE values with a trailing " 00:00:00.000000".
func TestRenderJSONAsMySQLText_OpaqueDate(t *testing.T) {
	payload := encodeJSONDateTimePayload(t, 2024, 1, 15, 0, 0, 0, 0)
	data := append([]byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_DATE, 0x08}, payload...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `"2024-01-15"`, string(out))
}

// TestRenderJSONAsMySQLText_OpaqueTime exercises the TIME path.
func TestRenderJSONAsMySQLText_OpaqueTime(t *testing.T) {
	payload := encodeJSONTimePayload(t, 10, 30, 45, 123456)
	data := append([]byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_TIME, 0x08}, payload...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `"10:30:45.123456"`, string(out))
}

// TestRenderJSONAsMySQLText_IgnoreDecodeError verifies that with
// ignoreDecodeErr=true the renderer returns a *valid* JSON document
// ("null") rather than the half-written buffer it accumulated before
// hitting the error. This matches the legacy decoder, which returns
// Go nil -> json.Marshal -> "null" in the same scenario.
func TestRenderJSONAsMySQLText_IgnoreDecodeError(t *testing.T) {
	// SMALL_OBJECT body that claims size=10 but only supplies 4 bytes.
	data := []byte{JSONB_SMALL_OBJECT, 0x00, 0x00, 0x0A, 0x00}

	_, err := renderJSONAsMySQLText(data, false)
	require.Error(t, err)

	out, err := renderJSONAsMySQLText(data, true)
	require.NoError(t, err)
	require.Equal(t, "null", string(out))

	// Empty data is also a valid JSON "null" when errors are ignored.
	out, err = renderJSONAsMySQLText(nil, true)
	require.NoError(t, err)
	require.Equal(t, "null", string(out))
}

func TestBinlogParser_SetRenderJSONAsMySQLText(t *testing.T) {
	parser := NewBinlogParser()
	require.False(t, parser.renderJSONAsMySQLText)
	parser.SetRenderJSONAsMySQLText(true)
	require.True(t, parser.renderJSONAsMySQLText)
	parser.SetRenderJSONAsMySQLText(false)
	require.False(t, parser.renderJSONAsMySQLText)
}
