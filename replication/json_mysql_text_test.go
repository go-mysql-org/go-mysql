package replication

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// renderJSONAsMySQLText is a test-only helper that synthesises a minimal
// RowsEvent so unit tests can drive jsonBinaryDecoder directly with the
// mysql-text mode enabled. Production code does not call this; it
// constructs RowsEvents via BinlogParser.newRowsEvent and reaches the
// same decoder through RowsEvent.decodeJSONBinary.
func renderJSONAsMySQLText(data []byte, ignoreDecodeErr bool) ([]byte, error) {
	e := &RowsEvent{
		renderJSONAsMySQLText: true,
		ignoreJSONDecodeErr:   ignoreDecodeErr,
	}
	return e.decodeJSONBinary(data)
}

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
		// Non-integer values: shortest round-trippable form. Note that
		// Go's 'g' emits "1.5e-05" where MySQL's my_gcvt emits "1.5e-5";
		// the float64 value (and therefore the re-stored JSONB) is
		// identical, only the visible text differs.
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
		// MySQL JSON is byte-transparent; invalid UTF-8 bytes are passed
		// through verbatim rather than replaced with U+FFFD.
		{"\xff\xfe", "\xff\xfe"},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		writeJSONString(&buf, []byte(c.in))
		require.Equal(t, c.want, buf.String(), "in=%q", c.in)
	}
}

// TestRenderJSONAsMySQLTextOpaqueUnknown covers the fallback branch for
// JSONB_OPAQUE values whose inner type is not one of the recognised MySQL
// temporal/decimal types. MySQL serialises these as "base64:typeNN:<b64>".
func TestRenderJSONAsMySQLTextOpaqueUnknown(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	// MYSQL_TYPE_BLOB (0xfc) is not handled specially by the decoder.
	data := append([]byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_BLOB, byte(len(payload))}, payload...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `"base64:type252:3q2+7w=="`, string(out))
}

// TestRenderJSONAsMySQLTextDouble exercises the renderer directly on a
// hand-constructed JSONB_DOUBLE byte stream (the simplest JSONB layout:
// a single scalar). It confirms the renderer emits MySQL-style "1.0" for
// a whole-number double instead of "1" (which the legacy decoder would
// produce because float64(1.0) marshals to "1" via json.Marshal).
//
// JSONB_OBJECT / JSONB_ARRAY / JSONB_OPAQUE layouts (offset tables,
// inline-vs-pointer values, NEWDECIMAL/DATETIME payloads) are covered by
// the fixture tests below. Full end-to-end coverage against a live MySQL
// binlog stream lives in github.com/block/spirit's pkg/repl tests.
func TestRenderJSONAsMySQLTextDouble(t *testing.T) {
	// JSONB top-level value: a single type byte followed by 8 bytes of
	// little-endian IEEE 754 representing 1.0.
	data := make([]byte, 1+8)
	data[0] = JSONB_DOUBLE
	binary.LittleEndian.PutUint64(data[1:], math.Float64bits(1.0))

	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "1.0", string(out))
}

// TestRenderJSONAsMySQLTextInt verifies that JSONB integers stay
// integers (no spurious ".0" decoration).
func TestRenderJSONAsMySQLTextInt(t *testing.T) {
	data := make([]byte, 1+2)
	data[0] = JSONB_INT16
	binary.LittleEndian.PutUint16(data[1:], uint16(int16(42)))

	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "42", string(out))
}

// TestRenderJSONAsMySQLTextLiteral exercises the LITERAL path.
func TestRenderJSONAsMySQLTextLiteral(t *testing.T) {
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

// TestRenderJSONAsMySQLTextEmptyObject and TestRenderJSONAsMySQLTextEmptyArray
// check the degenerate header-only cases.
func TestRenderJSONAsMySQLTextEmptyObject(t *testing.T) {
	// SMALL_OBJECT body: count=0 (LE u16), size=4 (LE u16) -- just the header.
	data := []byte{JSONB_SMALL_OBJECT, 0x00, 0x00, 0x04, 0x00}
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "{}", string(out))
}

func TestRenderJSONAsMySQLTextEmptyArray(t *testing.T) {
	data := []byte{JSONB_SMALL_ARRAY, 0x00, 0x00, 0x04, 0x00}
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "[]", string(out))
}

// TestRenderJSONAsMySQLTextObject exercises the SMALL_OBJECT offset table
// with one inline value (INT16) and one pointer value (STRING). This is
// the main path that risks getting offset arithmetic wrong.
func TestRenderJSONAsMySQLTextObject(t *testing.T) {
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
	require.Equal(t, `{"a":1,"b":"x"}`, string(out))
}

// TestRenderJSONAsMySQLTextArray exercises SMALL_ARRAY with a mix of
// inline scalars (INT16, LITERAL) and a pointer value (STRING).
func TestRenderJSONAsMySQLTextArray(t *testing.T) {
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
	require.Equal(t, `[1,"x",true]`, string(out))
}

// TestRenderJSONAsMySQLTextLargeObject exercises the LARGE_OBJECT
// offset table (4-byte offsets/counts) which the SMALL_OBJECT test
// does not cover. INT32 also becomes an inline value in large mode,
// where it was a pointer value in small mode.
func TestRenderJSONAsMySQLTextLargeObject(t *testing.T) {
	// Layout for {"a": 1, "b": "x"} as JSONB LARGE_OBJECT body:
	//   header (u32 count, u32 size)        = 8 bytes
	//   2 key entries (u32 offset, u16 len) = 12 bytes
	//   2 value entries (u8 tp, u32 slot)   = 10 bytes
	//   "a", "b"                            = 2 bytes
	//   varlen=1, 'x'                       = 2 bytes
	//   total body                          = 34 bytes
	//
	// Value 0 (INT16) is inline in large mode too, but the inline slot
	// is 4 bytes wide -- we use the first 2 and zero-pad the rest.
	body := []byte{
		0x02, 0x00, 0x00, 0x00, // count = 2
		0x22, 0x00, 0x00, 0x00, // size = 34
		// key entry 0: offset=30, len=1
		0x1E, 0x00, 0x00, 0x00, 0x01, 0x00,
		// key entry 1: offset=31, len=1
		0x1F, 0x00, 0x00, 0x00, 0x01, 0x00,
		// value entry 0: INT16 inline = 1 (zero-padded to 4 bytes)
		JSONB_INT16, 0x01, 0x00, 0x00, 0x00,
		// value entry 1: STRING at offset 32
		JSONB_STRING, 0x20, 0x00, 0x00, 0x00,
		'a', 'b',
		0x01, 'x',
	}
	data := append([]byte{JSONB_LARGE_OBJECT}, body...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `{"a":1,"b":"x"}`, string(out))
}

// TestRenderJSONAsMySQLTextLargeArray exercises the LARGE_ARRAY path
// with a mix of inline scalars (INT16, LITERAL) and a pointer value
// (STRING).
func TestRenderJSONAsMySQLTextLargeArray(t *testing.T) {
	// Layout for [1, "x", true] as JSONB LARGE_ARRAY body:
	//   header (u32 count, u32 size)      = 8 bytes
	//   3 value entries (u8 tp, u32 slot) = 15 bytes
	//   varlen=1, 'x'                     = 2 bytes
	//   total body                        = 25 bytes
	body := []byte{
		0x03, 0x00, 0x00, 0x00, // count = 3
		0x19, 0x00, 0x00, 0x00, // size = 25
		// value entry 0: INT16 inline = 1
		JSONB_INT16, 0x01, 0x00, 0x00, 0x00,
		// value entry 1: STRING at offset 23
		JSONB_STRING, 0x17, 0x00, 0x00, 0x00,
		// value entry 2: LITERAL inline true
		JSONB_LITERAL, JSONB_TRUE_LITERAL, 0x00, 0x00, 0x00,
		0x01, 'x',
	}
	data := append([]byte{JSONB_LARGE_ARRAY}, body...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `[1,"x",true]`, string(out))
}

// TestRenderJSONAsMySQLTextNestedObject exercises a jsonObject value
// nested inside another jsonObject. This is the path that would break
// if jsonObject.MarshalJSON ever stopped recursing through json.Marshal
// (e.g. if a future change started writing leaf bytes directly).
func TestRenderJSONAsMySQLTextNestedObject(t *testing.T) {
	// Inner SMALL_OBJECT body for {"inner": 1}:
	//   header (u16,u16) = 4
	//   1 key entry      = 4
	//   1 value entry    = 3
	//   "inner"          = 5
	//   total            = 16 bytes
	inner := []byte{
		0x01, 0x00, // count = 1
		0x10, 0x00, // size = 16
		0x0B, 0x00, 0x05, 0x00, // key entry: offset=11, len=5
		JSONB_INT16, 0x01, 0x00, // INT16 inline = 1
		'i', 'n', 'n', 'e', 'r',
	}
	// Outer SMALL_OBJECT body for {"outer": <inner>}:
	//   header        = 4
	//   1 key entry   = 4
	//   1 value entry = 3
	//   "outer"       = 5
	//   inner body    = 16
	//   total         = 32 bytes
	outer := []byte{
		0x01, 0x00, // count = 1
		0x20, 0x00, // size = 32
		0x0B, 0x00, 0x05, 0x00, // key entry: offset=11, len=5
		JSONB_SMALL_OBJECT, 0x10, 0x00, // value: SMALL_OBJECT at offset 16
		'o', 'u', 't', 'e', 'r',
	}
	outer = append(outer, inner...)
	data := append([]byte{JSONB_SMALL_OBJECT}, outer...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `{"outer":{"inner":1}}`, string(out))
}

// TestRenderJSONAsMySQLTextKeyEscaping verifies that object keys go
// through the same escaping path as string values: characters needing
// JSON escapes are escaped, and multi-byte UTF-8 passes through
// verbatim.
func TestRenderJSONAsMySQLTextKeyEscaping(t *testing.T) {
	// SMALL_OBJECT body for {"a\"b": 1, "héllo": 2}.
	// "a\"b" is 3 bytes; "héllo" is 6 bytes (é = 0xC3 0xA9 in UTF-8).
	//   header        = 4
	//   2 key entries = 8
	//   2 value entries = 6
	//   header end    = 18
	//   "a\"b" at 18, len 3
	//   "héllo" at 21, len 6
	//   total         = 27 bytes
	body := []byte{
		0x02, 0x00, // count = 2
		0x1B, 0x00, // size = 27
		0x12, 0x00, 0x03, 0x00, // key entry 0: offset=18, len=3
		0x15, 0x00, 0x06, 0x00, // key entry 1: offset=21, len=6
		JSONB_INT16, 0x01, 0x00, // INT16 inline = 1
		JSONB_INT16, 0x02, 0x00, // INT16 inline = 2
		'a', '"', 'b',
		'h', 0xC3, 0xA9, 'l', 'l', 'o',
	}
	data := append([]byte{JSONB_SMALL_OBJECT}, body...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, "{\"a\\\"b\":1,\"héllo\":2}", string(out))
}

// TestRenderJSONAsMySQLTextOpaqueDecimalShort exercises the bounds
// check on the NEWDECIMAL header. Previously a payload shorter than 2
// bytes would index past the slice; now it surfaces a decode error.
func TestRenderJSONAsMySQLTextOpaqueDecimalShort(t *testing.T) {
	data := []byte{
		JSONB_OPAQUE,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		0x01, // payload varlen = 1, but NEWDECIMAL needs >= 2
		0x02,
	}
	_, err := renderJSONAsMySQLText(data, false)
	require.Error(t, err)
}

// TestRenderJSONAsMySQLTextOpaqueDecimal exercises the NEWDECIMAL path
// inside JSONB_OPAQUE. This is the value class where the legacy decoder
// loses the JSON DECIMAL type (it becomes a quoted JSON STRING) and the
// renderer must keep the value unquoted.
func TestRenderJSONAsMySQLTextOpaqueDecimal(t *testing.T) {
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

// TestRenderJSONAsMySQLTextOpaqueDateTime exercises the DATETIME path.
// MySQL emits these quoted with microsecond resolution.
func TestRenderJSONAsMySQLTextOpaqueDateTime(t *testing.T) {
	payload := encodeJSONDateTimePayload(t, 2024, 1, 15, 10, 30, 45, 123456)
	data := append([]byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_DATETIME, 0x08}, payload...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `"2024-01-15 10:30:45.123456"`, string(out))
}

// TestRenderJSONAsMySQLTextOpaqueDate exercises the DATE path. The
// renderer is more correct here than the legacy decoder, which always
// formats DATE values with a trailing " 00:00:00.000000".
func TestRenderJSONAsMySQLTextOpaqueDate(t *testing.T) {
	payload := encodeJSONDateTimePayload(t, 2024, 1, 15, 0, 0, 0, 0)
	data := append([]byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_DATE, 0x08}, payload...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `"2024-01-15"`, string(out))
}

// TestRenderJSONAsMySQLTextOpaqueTime exercises the TIME path.
func TestRenderJSONAsMySQLTextOpaqueTime(t *testing.T) {
	payload := encodeJSONTimePayload(t, 10, 30, 45, 123456)
	data := append([]byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_TIME, 0x08}, payload...)
	out, err := renderJSONAsMySQLText(data, false)
	require.NoError(t, err)
	require.Equal(t, `"10:30:45.123456"`, string(out))
}

// TestRenderJSONAsMySQLTextIgnoreDecodeError verifies that with
// ignoreDecodeErr=true the renderer returns a *valid* JSON document
// ("null") rather than the half-written buffer it accumulated before
// hitting the error. This matches the legacy decoder, which returns
// Go nil -> json.Marshal -> "null" in the same scenario.
func TestRenderJSONAsMySQLTextIgnoreDecodeError(t *testing.T) {
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

func TestBinlogParserSetRenderJSONAsMySQLText(t *testing.T) {
	parser := NewBinlogParser()
	require.False(t, parser.renderJSONAsMySQLText)
	parser.SetRenderJSONAsMySQLText(true)
	require.True(t, parser.renderJSONAsMySQLText)
	parser.SetRenderJSONAsMySQLText(false)
	require.False(t, parser.renderJSONAsMySQLText)
}

// TestRenderJSONAsMySQLTextStringEscaping checks that a JSONB_STRING
// payload containing characters that JSON requires to be escaped
// (notably ", \, and control bytes) is emitted with the correct
// backslash escapes. The per-character escape logic lives in
// writeJSONString and is unit-tested directly by TestWriteJSONString;
// this case exercises it through jsonString.MarshalJSON to make sure
// the quote-and-escape sequencing in MarshalJSON is correct.
func TestRenderJSONAsMySQLTextStringEscaping(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"double_quote", `a"b`, `"a\"b"`},
		{"backslash", `a\b`, `"a\\b"`},
		{"both", `a"\b`, `"a\"\\b"`},
		{"newline", "a\nb", `"a\nb"`},
		{"control_byte", "a\x01b", `"a\u0001b"`},
		{"plain", "hello", `"hello"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := append([]byte{JSONB_STRING, byte(len(c.in))}, []byte(c.in)...)
			out, err := renderJSONAsMySQLText(data, false)
			require.NoError(t, err)
			require.Equal(t, c.want, string(out))
		})
	}
}

// TestRenderJSONAsMySQLTextOpaqueDispatch locks in which inner MySQL
// type bytes take a typed decode path inside JSONB_OPAQUE and which
// fall through to the "base64:typeN:..." envelope. The explicit set is
// small (NEWDECIMAL, TIME, DATE, DATETIME, TIMESTAMP); everything else
// must base64-encode. If a future change adds a new explicit branch in
// decodeOpaque, this table must be updated too -- otherwise the new
// branch is the silent kind of change that breaks downstream
// round-tripping.
func TestRenderJSONAsMySQLTextOpaqueDispatch(t *testing.T) {
	// Explicit cases: the output must NOT carry the base64 envelope.
	// Value formatting is covered by the dedicated Opaque{Decimal,
	// Date, DateTime, Time} tests above; here we only assert the
	// dispatch decision.
	explicit := []struct {
		name      string
		innerType byte
		payload   []byte
	}{
		{"NEWDECIMAL", mysql.MYSQL_TYPE_NEWDECIMAL, []byte{0x02, 0x01, 0x81, 0x00}},
		{"TIME", mysql.MYSQL_TYPE_TIME, encodeJSONTimePayload(t, 0, 0, 0, 0)},
		{"DATE", mysql.MYSQL_TYPE_DATE, encodeJSONDateTimePayload(t, 2024, 1, 15, 0, 0, 0, 0)},
		{"DATETIME", mysql.MYSQL_TYPE_DATETIME, encodeJSONDateTimePayload(t, 2024, 1, 15, 10, 30, 45, 0)},
		{"TIMESTAMP", mysql.MYSQL_TYPE_TIMESTAMP, encodeJSONDateTimePayload(t, 2024, 1, 15, 10, 30, 45, 0)},
	}
	for _, c := range explicit {
		t.Run("explicit/"+c.name, func(t *testing.T) {
			data := append([]byte{JSONB_OPAQUE, c.innerType, byte(len(c.payload))}, c.payload...)
			out, err := renderJSONAsMySQLText(data, false)
			require.NoError(t, err)
			require.False(t, strings.Contains(string(out), "base64:"),
				"type %s must not use base64 fallback, got %q", c.name, string(out))
		})
	}

	// Fallback cases: every other byte the decoder might see must be
	// emitted via the base64 envelope, matching mysqld's serialisation.
	// We assert the full output (not just the prefix) so a regression
	// that drops or mangles the trailing bytes is caught.
	fallback := []struct {
		name      string
		innerType byte
	}{
		{"DECIMAL", mysql.MYSQL_TYPE_DECIMAL},
		{"TINY", mysql.MYSQL_TYPE_TINY},
		{"SHORT", mysql.MYSQL_TYPE_SHORT},
		{"LONG", mysql.MYSQL_TYPE_LONG},
		{"FLOAT", mysql.MYSQL_TYPE_FLOAT},
		{"DOUBLE", mysql.MYSQL_TYPE_DOUBLE},
		{"NULL", mysql.MYSQL_TYPE_NULL},
		{"LONGLONG", mysql.MYSQL_TYPE_LONGLONG},
		{"INT24", mysql.MYSQL_TYPE_INT24},
		{"YEAR", mysql.MYSQL_TYPE_YEAR},
		{"NEWDATE", mysql.MYSQL_TYPE_NEWDATE},
		{"VARCHAR", mysql.MYSQL_TYPE_VARCHAR},
		{"BIT", mysql.MYSQL_TYPE_BIT},
		{"JSON", mysql.MYSQL_TYPE_JSON},
		{"ENUM", mysql.MYSQL_TYPE_ENUM},
		{"SET", mysql.MYSQL_TYPE_SET},
		{"TINY_BLOB", mysql.MYSQL_TYPE_TINY_BLOB},
		{"MEDIUM_BLOB", mysql.MYSQL_TYPE_MEDIUM_BLOB},
		{"LONG_BLOB", mysql.MYSQL_TYPE_LONG_BLOB},
		{"BLOB", mysql.MYSQL_TYPE_BLOB},
		{"VAR_STRING", mysql.MYSQL_TYPE_VAR_STRING},
		{"STRING", mysql.MYSQL_TYPE_STRING},
		{"GEOMETRY", mysql.MYSQL_TYPE_GEOMETRY},
	}
	payload := []byte{0xDE, 0xAD}
	const wantB64 = "3q0=" // base64.StdEncoding.EncodeToString([]byte{0xDE, 0xAD})
	for _, c := range fallback {
		t.Run("fallback/"+c.name, func(t *testing.T) {
			data := append([]byte{JSONB_OPAQUE, c.innerType, byte(len(payload))}, payload...)
			out, err := renderJSONAsMySQLText(data, false)
			require.NoError(t, err)
			want := `"base64:type` + strconv.Itoa(int(c.innerType)) + `:` + wantB64 + `"`
			require.Equal(t, want, string(out))
		})
	}
}
