package replication

import (
	"encoding/binary"
	"math"
	"testing"

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
		expectedType             interface{}
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.input.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, tt.expected, string(result))
		})
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
