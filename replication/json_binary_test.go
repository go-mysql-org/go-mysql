package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestFloatWithTrailingZero_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    FloatWithTrailingZero
		expected string
	}{
		{
			name:     "whole number should have .0",
			input:    FloatWithTrailingZero(5.0),
			expected: "5.0",
		},
		{
			name:     "negative whole number should have .0",
			input:    FloatWithTrailingZero(-3.0),
			expected: "-3.0",
		},
		{
			name:     "decimal number should preserve original format",
			input:    FloatWithTrailingZero(3.14),
			expected: "3.14",
		},
		{
			name:     "negative decimal should preserve original format",
			input:    FloatWithTrailingZero(-2.5),
			expected: "-2.5",
		},
		{
			name:     "zero should have .0",
			input:    FloatWithTrailingZero(0.0),
			expected: "0.0",
		},
		{
			name:     "very small decimal",
			input:    FloatWithTrailingZero(0.001),
			expected: "0.001",
		},
		{
			name:     "large whole number",
			input:    FloatWithTrailingZero(1000000.0),
			expected: "1000000.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.input.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, tt.expected, string(result))
		})
	}
}

func TestRegularFloat64_MarshalJSON_TruncatesTrailingZero(t *testing.T) {
	// Test that regular float64 truncates trailing zeros (the default behavior)
	// This demonstrates the difference when UseFloatWithTrailingZero is NOT set
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{
			name:     "whole number truncates .0",
			input:    5.0,
			expected: "5",
		},
		{
			name:     "negative whole number truncates .0",
			input:    -3.0,
			expected: "-3",
		},
		{
			name:     "decimal number preserves decimals",
			input:    3.14,
			expected: "3.14",
		},
		{
			name:     "negative decimal preserves decimals",
			input:    -2.5,
			expected: "-2.5",
		},
		{
			name:     "zero truncates .0",
			input:    0.0,
			expected: "0",
		},
		{
			name:     "large whole number truncates .0",
			input:    1000000.0,
			expected: "1000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := json.Marshal(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.expected, string(result))
		})
	}
}

func TestFloatWithTrailingZero_vs_RegularFloat64_Comparison(t *testing.T) {
	// Direct comparison test showing the key difference
	testCases := []struct {
		name            string
		value           float64
		withTrailing    string // Expected output with FloatWithTrailingZero
		withoutTrailing string // Expected output with regular float64
	}{
		{
			name:            "whole number 5.0",
			value:           5.0,
			withTrailing:    "5.0",
			withoutTrailing: "5",
		},
		{
			name:            "zero",
			value:           0.0,
			withTrailing:    "0.0",
			withoutTrailing: "0",
		},
		{
			name:            "negative whole number",
			value:           -42.0,
			withTrailing:    "-42.0",
			withoutTrailing: "-42",
		},
		{
			name:            "decimal number (no difference)",
			value:           3.14,
			withTrailing:    "3.14",
			withoutTrailing: "3.14",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test FloatWithTrailingZero
			trailingResult, err := json.Marshal(FloatWithTrailingZero(tc.value))
			require.NoError(t, err)
			require.Equal(t, tc.withTrailing, string(trailingResult))

			// Test regular float64
			regularResult, err := json.Marshal(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.withoutTrailing, string(regularResult))

			// Verify they're different for whole numbers
			if tc.value == float64(int(tc.value)) {
				require.NotEqual(t, string(trailingResult), string(regularResult),
					"FloatWithTrailingZero and regular float64 should produce different output for whole numbers")
			}
		})
	}
}

func TestJsonBinaryDecoder_decodeDoubleWithTrailingZero(t *testing.T) {
	// Test the decodeDoubleWithTrailingZero method directly
	decoder := &jsonBinaryDecoder{
		useFloatWithTrailingZero: true,
	}

	// Test data representing 5.0 as IEEE 754 double precision in little endian binary format
	testData := make([]byte, 8)
	binary.LittleEndian.PutUint64(testData, math.Float64bits(5.0))

	result := decoder.decodeDoubleWithTrailingZero(testData)
	require.NoError(t, decoder.err)

	// Verify the result is FloatWithTrailingZero type
	require.IsType(t, FloatWithTrailingZero(0), result)
	require.Equal(t, FloatWithTrailingZero(5.0), result)

	// Test JSON marshaling
	jsonBytes, err := json.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, "5.0", string(jsonBytes))
}

func TestJsonBinaryDecoder_decodeValue_JSONB_DOUBLE(t *testing.T) {
	tests := []struct {
		name                     string
		useFloatWithTrailingZero bool
		value                    float64
		expectedType             any
		expectedJSONString       string
	}{
		{
			name:                     "positive number with trailing zero enabled",
			useFloatWithTrailingZero: true,
			value:                    5.0,
			expectedType:             FloatWithTrailingZero(0),
			expectedJSONString:       "5.0",
		},
		{
			name:                     "positive number with trailing zero disabled",
			useFloatWithTrailingZero: false,
			value:                    5.0,
			expectedType:             float64(0),
			expectedJSONString:       "5",
		},
		{
			name:                     "negative zero with trailing zero enabled",
			useFloatWithTrailingZero: true,
			value:                    math.Copysign(0.0, -1),
			expectedType:             FloatWithTrailingZero(0),
			expectedJSONString:       "-0.0",
		},
		{
			name:                     "negative zero with trailing zero disabled",
			useFloatWithTrailingZero: false,
			value:                    math.Copysign(0.0, -1),
			expectedType:             float64(0),
			expectedJSONString:       "-0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test data as IEEE 754 double precision in little endian binary format
			testData := make([]byte, 8)
			binary.LittleEndian.PutUint64(testData, math.Float64bits(tt.value))

			decoder := &jsonBinaryDecoder{
				useFloatWithTrailingZero: tt.useFloatWithTrailingZero,
			}

			result := decoder.decodeValue(JSONB_DOUBLE, testData)
			require.NoError(t, decoder.err)
			require.IsType(t, tt.expectedType, result)

			// Test JSON marshaling
			jsonBytes, err := json.Marshal(result)
			require.NoError(t, err)
			require.Equal(t, tt.expectedJSONString, string(jsonBytes))
		})
	}
}

func TestBinlogParser_SetUseFloatWithTrailingZero(t *testing.T) {
	parser := NewBinlogParser()

	// Test default value
	require.False(t, parser.useFloatWithTrailingZero)

	// Test setting to true
	parser.SetUseFloatWithTrailingZero(true)
	require.True(t, parser.useFloatWithTrailingZero)

	// Test setting to false
	parser.SetUseFloatWithTrailingZero(false)
	require.False(t, parser.useFloatWithTrailingZero)
}

func TestFloatWithTrailingZero_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    FloatWithTrailingZero
		expected string
	}{
		{
			name:     "very large whole number",
			input:    FloatWithTrailingZero(1e15),
			expected: "1000000000000000.0",
		},
		{
			name:     "very small positive number",
			input:    FloatWithTrailingZero(1e-10),
			expected: "0.0000000001",
		},
		{
			name:     "scientific notation threshold",
			input:    FloatWithTrailingZero(1e6),
			expected: "1000000.0",
		},
		{
			name:     "number that looks whole but has tiny fractional part",
			input:    FloatWithTrailingZero(5.0000000000000001), // This might be rounded to 5.0 due to float64 precision
			expected: "5.0",
		},
		{
			name:     "largest double below 2^63",
			input:    FloatWithTrailingZero(9223372036854774784),
			expected: "9223372036854774784.0",
		},
		{
			name:     "2^63",
			input:    FloatWithTrailingZero(9223372036854775808),
			expected: "9.223372036854776e18",
		},
		{
			name:     "-2^63",
			input:    FloatWithTrailingZero(-9223372036854775808),
			expected: "-9.223372036854776e18",
		},
		{
			name:     "1e19",
			input:    FloatWithTrailingZero(1e19),
			expected: "1e19",
		},
		{
			name:     "-1e19",
			input:    FloatWithTrailingZero(-1e19),
			expected: "-1e19",
		},
		{
			name:     "6.02214076e23",
			input:    FloatWithTrailingZero(6.02214076e23),
			expected: "6.02214076e23",
		},
		{
			name:     "1e300",
			input:    FloatWithTrailingZero(1e300),
			expected: "1e300",
		},
		{
			name:     "largest double",
			input:    FloatWithTrailingZero(math.MaxFloat64),
			expected: "1.7976931348623157e308",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.input.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, tt.expected, string(result))
		})
	}
}

func TestFloatWithTrailingZero_LargeValues(t *testing.T) {
	// MySQL reads a JSON number without '.' or 'e' as an integer when it fits
	// in 64 bits, and MySQL 8.4 misreads long digit strings, so every value
	// must come out short and with a '.' or an 'e'.
	var values []float64
	for e := 0; e <= 1023; e++ {
		values = append(values, math.Ldexp(1, e))
	}
	for e := 0; e <= 308; e++ {
		values = append(values, math.Pow10(e))
	}
	values = append(values, math.Nextafter(0x1p64, 0), math.MaxFloat64)

	for _, v := range values {
		for _, x := range []float64{v, -v} {
			result, err := FloatWithTrailingZero(x).MarshalJSON()
			require.NoError(t, err)
			s := string(result)
			require.True(t, strings.ContainsAny(s, ".e"), "%g: %s", x, s)
			require.LessOrEqual(t, len(s), 24, "%g: %s", x, s)
			parsed, err := strconv.ParseFloat(s, 64)
			require.NoError(t, err)
			require.Equal(t, x, parsed, s)
		}
	}
}

func TestFloatWithTrailingZero_NonFinite(t *testing.T) {
	// NaN and ±Inf stay invalid JSON, as for float64; they must not turn
	// into the null that formatMySQLDouble writes for them.
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := json.Marshal(FloatWithTrailingZero(v))
		require.Error(t, err, "%v", v)
	}
}

func TestRowsEvent_decodeJSONBinary_FloatWithTrailingZero(t *testing.T) {
	tests := []struct {
		value    float64
		expected string
	}{
		{5, "5.0"},
		{1e19, "1e19"},
		{-1.5e300, "-1.5e300"},
	}

	e := &RowsEvent{useFloatWithTrailingZero: true}
	for _, tt := range tests {
		// A JSONB scalar: the type byte, then the little-endian double.
		data := make([]byte, 9)
		data[0] = JSONB_DOUBLE
		binary.LittleEndian.PutUint64(data[1:], math.Float64bits(tt.value))

		result, err := e.decodeJSONBinary(data)
		require.NoError(t, err)
		require.Equal(t, tt.expected, string(result))
	}
}

func TestFloatWithTrailingZero_Integration(t *testing.T) {
	// Test that demonstrates the full flow with a sample JSON structure
	type JSONData struct {
		Price    FloatWithTrailingZero `json:"price"`
		Quantity FloatWithTrailingZero `json:"quantity"`
		Total    FloatWithTrailingZero `json:"total"`
	}

	data := JSONData{
		Price:    FloatWithTrailingZero(10.0), // Should become "10.0"
		Quantity: FloatWithTrailingZero(2.5),  // Should stay "2.5"
		Total:    FloatWithTrailingZero(25.0), // Should become "25.0"
	}

	jsonBytes, err := json.Marshal(data)
	require.NoError(t, err)

	expectedJSON := `{"price":10.0,"quantity":2.5,"total":25.0}`
	require.Equal(t, expectedJSON, string(jsonBytes))

	// Verify that regular float64 would behave differently
	type RegularJSONData struct {
		Price    float64 `json:"price"`
		Quantity float64 `json:"quantity"`
		Total    float64 `json:"total"`
	}

	regularData := RegularJSONData{
		Price:    10.0,
		Quantity: 2.5,
		Total:    25.0,
	}

	regularJSONBytes, err := json.Marshal(regularData)
	require.NoError(t, err)

	regularExpectedJSON := `{"price":10,"quantity":2.5,"total":25}`
	require.Equal(t, regularExpectedJSON, string(regularJSONBytes))

	// Demonstrate the difference
	require.NotEqual(t, string(jsonBytes), string(regularJSONBytes))
}

func TestRowsEvent_UseFloatWithTrailingZero_Integration(t *testing.T) {
	// Test that RowsEvent properly propagates the useFloatWithTrailingZero setting

	// Create table map event (similar to existing tests in replication_test.go)
	tableMapEventData := []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x03t10\x00\x02\xf5\xf6\x03\x04\n\x00\x03")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	require.Greater(t, tableMapEvent.TableID, uint64(0))

	// Test with useFloatWithTrailingZero enabled
	rowsWithTrailingZero := &RowsEvent{
		tables:                   make(map[uint64]*TableMapEvent),
		useFloatWithTrailingZero: true,
	}
	rowsWithTrailingZero.tables[tableMapEvent.TableID] = tableMapEvent

	// Test with useFloatWithTrailingZero disabled
	rowsWithoutTrailingZero := &RowsEvent{
		tables:                   make(map[uint64]*TableMapEvent),
		useFloatWithTrailingZero: false,
	}
	rowsWithoutTrailingZero.tables[tableMapEvent.TableID] = tableMapEvent

	// Verify that the setting is properly stored
	require.True(t, rowsWithTrailingZero.useFloatWithTrailingZero)
	require.False(t, rowsWithoutTrailingZero.useFloatWithTrailingZero)

	// Test the decoder creation with the setting
	decoderWithTrailing := &jsonBinaryDecoder{
		useFloatWithTrailingZero: rowsWithTrailingZero.useFloatWithTrailingZero,
	}

	decoderWithoutTrailing := &jsonBinaryDecoder{
		useFloatWithTrailingZero: rowsWithoutTrailingZero.useFloatWithTrailingZero,
	}

	require.True(t, decoderWithTrailing.useFloatWithTrailingZero)
	require.False(t, decoderWithoutTrailing.useFloatWithTrailingZero)
}

// jsonbNestedArrays returns n arrays nested in each other, [[...[]...]], in
// the layout MySQL writes, for n up to 9,000.
func jsonbNestedArrays(n int) []byte {
	doc := []byte{0x00, 0x00, 0x04, 0x00} // []
	for range n - 1 {
		size := 7 + len(doc)
		doc = append([]byte{0x01, 0x00, byte(size), byte(size >> 8), JSONB_SMALL_ARRAY, 0x07, 0x00}, doc...)
	}
	return append([]byte{JSONB_SMALL_ARRAY}, doc...)
}

// jsonbNestedObjects returns n objects nested in each other,
// {"a":{"a":...{}...}}, in the layout MySQL writes, for n up to 5,000.
func jsonbNestedObjects(n int) []byte {
	doc := []byte{0x00, 0x00, 0x04, 0x00} // {}
	for range n - 1 {
		size := 12 + len(doc)
		doc = append([]byte{0x01, 0x00, byte(size), byte(size >> 8), 0x0b, 0x00, 0x01, 0x00, JSONB_SMALL_OBJECT, 0x0c, 0x00, 'a'}, doc...)
	}
	return append([]byte{JSONB_SMALL_OBJECT}, doc...)
}

// jsonbSharedArrays returns n levels of [x, x], where both entries of each
// array point at the same child, ending in [1, 1]. Decoded naively, its
// JSON text doubles with every level.
func jsonbSharedArrays(n int) []byte {
	doc := []byte{JSONB_SMALL_ARRAY}
	for i := range n {
		size := 10*(n-i) + 10
		doc = append(doc, 0x02, 0x00, byte(size), byte(size>>8),
			JSONB_SMALL_ARRAY, 0x0a, 0x00, JSONB_SMALL_ARRAY, 0x0a, 0x00)
	}
	return append(doc, 0x02, 0x00, 0x0a, 0x00, JSONB_INT16, 0x01, 0x00, JSONB_INT16, 0x01, 0x00)
}

// jsonbSharedChild returns a small array or object with n members whose
// value entries (and, for an object, key entries) all point at the same
// bytes: a string of length 100, and the key "k".
func jsonbSharedChild(n int, isObject bool) []byte {
	tp, entries := JSONB_SMALL_ARRAY, 3*n
	if isObject {
		tp, entries = JSONB_SMALL_OBJECT, 7*n
	}
	header := 4 + entries
	keyOffset, valueOffset := header, header
	if isObject {
		valueOffset++ // after the key
	}
	doc := []byte{tp, byte(n), byte(n >> 8), 0, 0}
	if isObject {
		for range n {
			doc = append(doc, byte(keyOffset), byte(keyOffset>>8), 0x01, 0x00)
		}
	}
	for range n {
		doc = append(doc, JSONB_STRING, byte(valueOffset), byte(valueOffset>>8))
	}
	if isObject {
		doc = append(doc, 'k')
	}
	doc = append(doc, 100)
	doc = append(doc, strings.Repeat("s", 100)...)
	size := len(doc) - 1
	doc[3], doc[4] = byte(size), byte(size>>8)
	return doc
}

func TestJSONBinaryOffsetOutsideContainer(t *testing.T) {
	tests := []struct {
		name string
		doc  []byte // a valid document
		want string // its JSON
		at   int    // the byte to change
		to   byte   // its new value
		err  string
	}{
		{
			name: "value points at its own array",
			doc:  []byte{JSONB_SMALL_ARRAY, 0x01, 0x00, 0x0b, 0x00, JSONB_SMALL_ARRAY, 0x07, 0x00, 0x00, 0x00, 0x04, 0x00},
			want: "[[]]",
			at:   6, // value offset of the entry, 7
			to:   0,
			err:  "invalid value offset 0",
		},
		{
			name: "value starts after the end of its array",
			doc: []byte{
				JSONB_SMALL_ARRAY, 0x02, 0x00, 0x15, 0x00,
				JSONB_SMALL_ARRAY, 0x0a, 0x00, JSONB_STRING, 0x13, 0x00,
				0x01, 0x00, 0x09, 0x00, JSONB_STRING, 0x07, 0x00, 0x01, 't',
				0x01, 's',
			},
			want: `[["t"],"s"]`,
			at:   13, // size of the inner array, 9; "t" stays after its new end
			to:   7,
			err:  "invalid value offset 7",
		},
		{
			name: "string runs past the end of its array",
			doc: []byte{
				JSONB_SMALL_ARRAY, 0x02, 0x00, 0x15, 0x00,
				JSONB_SMALL_ARRAY, 0x0a, 0x00, JSONB_STRING, 0x13, 0x00,
				0x01, 0x00, 0x09, 0x00, JSONB_STRING, 0x07, 0x00, 0x01, 't',
				0x01, 's',
			},
			want: `[["t"],"s"]`,
			at:   13, // size of the inner array, 9; now ends after the length of "t"
			to:   8,
			err:  "data len 1 < expected 2",
		},
		{
			name: "number runs past the end of its array",
			doc: []byte{
				JSONB_SMALL_ARRAY, 0x02, 0x00, 0x17, 0x00,
				JSONB_SMALL_ARRAY, 0x0a, 0x00, JSONB_STRING, 0x15, 0x00,
				0x01, 0x00, 0x0b, 0x00, JSONB_INT32, 0x07, 0x00, 0xa0, 0x86, 0x01, 0x00,
				0x01, 's',
			},
			want: `[[100000],"s"]`,
			at:   13, // size of the inner array, 11; now ends after 2 bytes of 100000
			to:   9,
			err:  "data len 2 < expected 4",
		},
		{
			name: "opaque value runs past the end of its array",
			doc: []byte{
				JSONB_SMALL_ARRAY, 0x02, 0x00, 0x1d, 0x00,
				JSONB_SMALL_ARRAY, 0x0a, 0x00, JSONB_STRING, 0x1b, 0x00,
				0x01, 0x00, 0x11, 0x00, JSONB_OPAQUE, 0x07, 0x00,
				mysql.MYSQL_TYPE_TIME, 0x08, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00,
				0x01, 's',
			},
			want: `[["12:00:00.000000"],"s"]`,
			at:   13, // size of the inner array, 17; now ends after 3 bytes of the TIME
			to:   12,
			err:  "data len 4 < expected 9",
		},
		{
			name: "decimal shorter than its precision needs",
			doc: []byte{
				JSONB_SMALL_ARRAY, 0x02, 0x00, 0x19, 0x00,
				JSONB_SMALL_ARRAY, 0x0a, 0x00, JSONB_STRING, 0x17, 0x00,
				0x01, 0x00, 0x0d, 0x00, JSONB_OPAQUE, 0x07, 0x00,
				mysql.MYSQL_TYPE_NEWDECIMAL, 0x04, 0x04, 0x00, 0x84, 0xd2, // DECIMAL(4, 0) 1234
				0x01, 's',
			},
			want: `[["1234"],"s"]`,
			at:   19, // length of the DECIMAL, 4; now ends after its precision and scale
			to:   2,
			err:  "data len 0 < expected 2",
		},
		{
			name: "key runs past the end of its object",
			doc: []byte{
				JSONB_SMALL_ARRAY, 0x02, 0x00, 0x19, 0x00,
				JSONB_SMALL_OBJECT, 0x0a, 0x00, JSONB_STRING, 0x16, 0x00,
				0x01, 0x00, 0x0c, 0x00, 0x0b, 0x00, 0x01, 0x00, JSONB_INT16, 0x01, 0x00, 'a',
				0x02, 'b', 'c',
			},
			want: `[{"a":1},"bc"]`,
			at:   17, // length of the key "a", 1; the key then runs into "bc"
			to:   3,
			err:  "invalid key offset 11 length 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := (&RowsEvent{}).decodeJSONBinary(tt.doc)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(out))

			doc := bytes.Clone(tt.doc)
			doc[tt.at] = tt.to
			_, err = (&RowsEvent{}).decodeJSONBinary(doc)
			require.ErrorContains(t, err, tt.err)
			_, err = renderJSONAsMySQLText(doc, false)
			require.ErrorContains(t, err, tt.err)

			out, err = (&RowsEvent{ignoreJSONDecodeErr: true}).decodeJSONBinary(doc)
			require.NoError(t, err)
			require.Equal(t, "null", string(out))
		})
	}

	// {"": 1} as MySQL writes it: the empty key starts at the end of the object.
	out, err := (&RowsEvent{}).decodeJSONBinary([]byte{JSONB_SMALL_OBJECT, 0x01, 0x00, 0x0b, 0x00, 0x0b, 0x00, 0x00, 0x00, JSONB_INT16, 0x01, 0x00})
	require.NoError(t, err)
	require.Equal(t, `{"":1}`, string(out))
}

func TestJSONBinaryLengthOutOfRange(t *testing.T) {
	// Both used to overflow int where it has 32 bits: the header size of
	// the object wrapped around to a negative number, and so did the length
	// of the string.
	tests := []struct {
		name string
		doc  []byte
		err  string
	}{
		{
			name: "large object with 201,326,592 members in 8 bytes",
			doc:  []byte{JSONB_LARGE_OBJECT, 0x00, 0x00, 0x00, 0x0c, 0x08, 0x00, 0x00, 0x00},
			err:  "> size 8",
		},
		{
			name: "string of 4,294,967,295 bytes",
			doc:  []byte{JSONB_STRING, 0xff, 0xff, 0xff, 0xff, 0x0f},
			err:  "data len 5 < expected 4294967300",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := (&RowsEvent{}).decodeJSONBinary(tt.doc)
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestJSONBinaryMaxDepth(t *testing.T) {
	// MySQL refuses to serialize a document nested deeper than 100 levels,
	// but stores one when JSON_SET or JSON_REPLACE puts values into it in
	// place.
	modes := []*RowsEvent{{}, {renderJSONAsMySQLText: true}}
	for _, n := range []int{101, jsonbMaxDepth} {
		for _, e := range modes {
			out, err := e.decodeJSONBinary(jsonbNestedArrays(n))
			require.NoError(t, err)
			require.Equal(t, strings.Repeat("[", n)+strings.Repeat("]", n), string(out))

			out, err = e.decodeJSONBinary(jsonbNestedObjects(n))
			require.NoError(t, err)
			require.Equal(t, strings.Repeat(`{"a":`, n-1)+"{}"+strings.Repeat("}", n-1), string(out))
		}
	}

	for _, doc := range [][]byte{jsonbNestedArrays(jsonbMaxDepth + 1), jsonbNestedObjects(jsonbMaxDepth + 1)} {
		for _, e := range modes {
			_, err := e.decodeJSONBinary(doc)
			require.ErrorContains(t, err, "nested deeper than 1000 levels")
		}
	}
}

func TestJSONBinaryOversizedChildIgnored(t *testing.T) {
	// The inner array declares 8 bytes, but its parent ends after the first
	// 4 (its count and size). Its string entry and the string's length (2)
	// follow the parent; the string bytes are missing.
	doc := []byte{
		JSONB_SMALL_ARRAY, 0x01, 0x00, 0x0b, 0x00, JSONB_SMALL_ARRAY, 0x07, 0x00,
		0x01, 0x00, 0x08, 0x00, JSONB_STRING, 0x07, 0x00, 0x02,
	}
	_, err := (&RowsEvent{}).decodeJSONBinary(doc)
	require.ErrorContains(t, err, "data len 4 < expected 8")

	// With IgnoreJSONDecodeError, the existing exception for a container
	// larger than its data (MySQL bug 88791) makes only that child null.
	out, err := (&RowsEvent{ignoreJSONDecodeErr: true}).decodeJSONBinary(doc)
	require.NoError(t, err)
	require.Equal(t, "[null]", string(out))
}

func TestJSONBinaryDecimalMetadata(t *testing.T) {
	// DECIMAL(2, 2) 0.12: the scale may equal the precision.
	doc := []byte{JSONB_OPAQUE, mysql.MYSQL_TYPE_NEWDECIMAL, 0x03, 0x02, 0x02, 0x8c}
	out, err := (&RowsEvent{}).decodeJSONBinary(doc)
	require.NoError(t, err)
	require.Equal(t, `"0.12"`, string(out))
	out, err = renderJSONAsMySQLText(doc, false)
	require.NoError(t, err)
	require.Equal(t, `0.12`, string(out))

	for _, doc := range [][]byte{
		{JSONB_OPAQUE, mysql.MYSQL_TYPE_NEWDECIMAL, 0x02, 0x00, 0x00},       // precision 0
		{JSONB_OPAQUE, mysql.MYSQL_TYPE_NEWDECIMAL, 0x03, 0x01, 0x02, 0x8c}, // scale above precision
	} {
		_, err := (&RowsEvent{}).decodeJSONBinary(doc)
		require.ErrorContains(t, err, "invalid decimal precision")
	}
}

func TestJSONBinaryOverlappingValues(t *testing.T) {
	tests := []struct {
		name string
		doc  []byte
	}{
		{"arrays shared on every level", jsonbSharedArrays(16)},
		{"one string for every array element", jsonbSharedChild(1000, false)},
		{"one key and string for every object member", jsonbSharedChild(1000, true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, e := range []*RowsEvent{{}, {renderJSONAsMySQLText: true}} {
				out, err := e.decodeJSONBinary(tt.doc)
				require.ErrorContains(t, err, "values overlap", "%d bytes of JSON from %d bytes", len(out), len(tt.doc))
			}
		})
	}

	// Without the shared levels, and with one member, nothing is shared.
	out, err := (&RowsEvent{}).decodeJSONBinary(jsonbSharedArrays(0))
	require.NoError(t, err)
	require.Equal(t, "[1,1]", string(out))
	out, err = renderJSONAsMySQLText(jsonbSharedChild(1, true), false)
	require.NoError(t, err)
	require.Equal(t, `{"k":"`+strings.Repeat("s", 100)+`"}`, string(out))
}

func TestJSONPartialBinaryInvalidValue(t *testing.T) {
	for _, tc := range []struct {
		value []byte
		err   string
	}{
		{
			value: []byte{JSONB_SMALL_ARRAY, 0x01, 0x00, 0x0b, 0x00, JSONB_SMALL_ARRAY, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00},
			err:   "cannot read json diff value (op Replace, 12 bytes): invalid value offset 0, must be within [7, 11)",
		},
		{
			value: jsonbNestedArrays(jsonbMaxDepth + 1),
			err:   "cannot read json diff value (op Replace, 7005 bytes): JSON document is nested deeper than 1000 levels",
		},
	} {
		// JSON_REPLACE(j, '$.a', value)
		diff := append([]byte{byte(JsonDiffOperationReplace), 3}, "$.a"...)
		diff = append(diff, mysql.PutLengthEncodedInt(uint64(len(tc.value)))...)
		diff = append(diff, tc.value...)

		_, err := (&RowsEvent{}).decodeJSONPartialBinary(diff)
		require.EqualError(t, err, tc.err)

		d, err := (&RowsEvent{ignoreJSONDecodeErr: true}).decodeJSONPartialBinary(diff)
		require.NoError(t, err)
		require.Equal(t, "null", d.Value)
	}
}

func FuzzJSONBinaryDecode(f *testing.F) {
	// Documents written by MySQL 9.7.4.
	for _, s := range []string{
		// 18446744073709551615
		"0affffffffffffffff",
		// 1.5
		"0b000000000000f83f",
		// a string with escapes
		"0c0d00011f0a090d080c225c2f207f",
		// a string with non-ASCII characters
		"0c15636166c3a920e2988320f09d849e20d0b0d0b1d0b2",
		// an array of numbers of every size, literals, a double and a string
		"021000620005000005ffff05ff7f050080073400073800073c000740000744000948000a50000400000401000402000b58000c6000ffff000000000100ff7fffffffffff7f00000080ffffffff000000000000000001000000000000000000f03f0173",
		// an object with members of every type
		"000a0076004a0001004b0001004c0001004d0001004e0001004f000100500001005100010052000200540002000501000a56000401000402000b5e000c6600026a00006e00077200040000616365666768696a626264640000000001000000000000000000044003737472000004000000040090eefeff",
		// an object with keys of different lengths
		"0007003e0035000000350001003600010037000100380002003a0002003c00020005050005070005020005010005030005040005060042616261616162c3a9",
		// nested arrays and objects
		"00020072001200010013000100021400005600616502004200050100000a00010038000b000100020c006202002c00050200000a00010022000b000100020c006302001600050300000a0001000c000b0001000400006401001c000b000100000c0066010010000b000100000c006700000400",
		// DECIMAL values
		"000200260012000100130001000f14000f1e006465f6080d048000007b1194f60609007ffffffe",
		// DATE, TIME and DATETIME values
		"0207005f000f19000f23000f2d000f37000f41000f4b000f55000a0800000000006cbb190b080000000591cbffff0b08000000fb6e3400000b0800000000000000000c0840e20100a06cbb190c083f420ffb7efff37e0a08000000000042b20c",
		// TIMESTAMP, YEAR, BIT and binary string values
		"02070038000f190006ea070f23000f27000f2c000c32000c3400070840e20100a06cbb1910020aaa0f0300ff10fc04deadbeef017903612c62",
		// a TIME
		"0f0b0820a107b8c8000000",
	} {
		doc, err := hex.DecodeString(s)
		require.NoError(f, err)
		f.Add(doc)
	}
	f.Add(jsonbNestedArrays(101))
	f.Add(jsonbSharedArrays(16))
	f.Add(jsonbSharedChild(100, true))
	f.Add([]byte{JSONB_SMALL_ARRAY, 0x01, 0x00, 0x0b, 0x00, JSONB_SMALL_ARRAY, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00})
	// {"a": 1, "b": "x"} as a large object
	f.Add([]byte{
		JSONB_LARGE_OBJECT, 0x02, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00,
		0x1e, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1f, 0x00, 0x00, 0x00, 0x01, 0x00,
		JSONB_INT16, 0x01, 0x00, 0x00, 0x00, JSONB_STRING, 0x20, 0x00, 0x00, 0x00,
		'a', 'b', 0x01, 'x',
	})

	f.Fuzz(func(t *testing.T, doc []byte) {
		// The document as the JSON column of a row, decoded the way callers
		// decode it.
		row := binary.LittleEndian.AppendUint32([]byte{0x00}, uint32(len(doc)))
		row = append(row, doc...)
		for _, tt := range []struct {
			e *RowsEvent
			// The most bytes of JSON per byte of document. Values are
			// decoded once per entry, and an entry of 3 bytes can point at a
			// double shared with other entries: up to 35 bytes of JSON in
			// the default and MySQL text modes, and up to about 330 in
			// FloatWithTrailingZero's fixed-point notation.
			ratio int
		}{
			{&RowsEvent{}, 16},
			{&RowsEvent{renderJSONAsMySQLText: true}, 16},
			{&RowsEvent{useDecimal: true, useFloatWithTrailingZero: true}, 128},
		} {
			e := tt.e
			e.eventType = WRITE_ROWS_EVENTv2
			e.Table = &TableMapEvent{ColumnType: []byte{mysql.MYSQL_TYPE_JSON}, ColumnMeta: []uint16{4}}
			e.ColumnCount = 1
			e.ColumnBitmap1 = []byte{0x01}
			if err := e.DecodeData(0, row); err != nil {
				continue
			}
			out, _ := e.Rows[0][0].(string)
			// In uint64, so that it cannot overflow where int has 32 bits.
			if uint64(len(out)) > uint64(tt.ratio)*uint64(len(doc))+16 {
				t.Fatalf("%d bytes of JSON from a %d-byte document", len(out), len(doc))
			}
		}
	})
}
