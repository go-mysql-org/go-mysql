package mysql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnNumber(t *testing.T) {
	r := NewResultReserveResultset(0)
	// Make sure ColumnNumber doesn't panic when constructing a Result with 0
	// columns. https://github.com/go-mysql-org/go-mysql/issues/964
	r.ColumnNumber()
}

// TestGetInt tests GetInt with a negative value
func TestGetIntNeg(t *testing.T) {
	r := NewResultset(1)
	fv := NewFieldValue(FieldValueTypeString, 0, []uint8("-193"))
	r.Values = [][]FieldValue{{fv}}
	v, err := r.GetInt(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(-193), v)
}

func TestGetIntByName(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   FieldValue
		want    int64
		wantErr bool
	}{
		{"negative string", NewFieldValue(FieldValueTypeString, 0, []byte("-193")), -193, false},
		{"negative float", NewFieldValue(FieldValueTypeFloat, math.Float64bits(-42.5), nil), -42, false},
		{"negative integer", NewFieldValue(FieldValueTypeSigned, math.MaxUint64, nil), -1, false},
		{"positive string", NewFieldValue(FieldValueTypeString, 0, []byte("42")), 42, false},
		{"minimum integer", NewFieldValue(FieldValueTypeString, 0, []byte("-9223372036854775808")), math.MinInt64, false},
		{"overflow", NewFieldValue(FieldValueTypeString, 0, []byte("9223372036854775808")), 0, true},
		{"invalid string", NewFieldValue(FieldValueTypeString, 0, []byte("invalid")), 0, true},
		{"null", NewFieldValue(FieldValueTypeNull, 0, nil), 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := NewResultset(1)
			r.FieldNames["value"] = 0
			r.Values = [][]FieldValue{{tt.value}}
			want, wantErr := r.GetInt(0, 0)
			got, err := r.GetIntByName(0, "value")
			if tt.wantErr {
				require.Error(t, wantErr)
				require.EqualError(t, err, wantErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			require.Equal(t, want, got)
		})
	}

	r := NewResultset(1)
	r.FieldNames["value"] = 0
	_, err := r.GetIntByName(0, "missing")
	require.Error(t, err)
	_, err = r.GetIntByName(0, "value")
	require.Error(t, err)
}

func TestBuildSimpleTextResultsetDistinguishesEmptyValuesFromNull(t *testing.T) {
	r, err := BuildSimpleTextResultset(
		[]string{"empty_string", "empty_bytes", "null", "bool"},
		[][]any{{"", []byte{}, nil, false}},
	)
	require.NoError(t, err)
	require.Equal(t, RowData{0x00, 0x00, 0xfb, 0x5, 0x66, 0x61, 0x6c, 0x73, 0x65}, r.RowDatas[0])
	require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[0].Type)
	require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[1].Type)
	require.Equal(t, MYSQL_TYPE_NULL, r.Fields[2].Type)
	require.Equal(t, MYSQL_TYPE_TINY, r.Fields[3].Type)
}

// A Resultset taken from the pool still has the previous result's *Field
// pointers in the backing array of Fields. BuildSimpleTextResultset must not
// treat them as fields of the new result.
// https://github.com/go-mysql-org/go-mysql/issues/1197
func TestBuildSimpleTextResultsetIgnoresPooledFields(t *testing.T) {
	for range 100 {
		stale := NewResultset(1)
		stale.Fields[0] = &Field{Name: []byte("stale"), Type: MYSQL_TYPE_LONGLONG}
		(&Result{Resultset: stale}).Close()

		r, err := BuildSimpleTextResultset([]string{"fresh"}, [][]any{{"x"}})
		require.NoError(t, err)
		require.Equal(t, "fresh", string(r.Fields[0].Name))
		require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[0].Type)
		(&Result{Resultset: r}).Close()
	}
}

func TestBuildSimpleBinaryResultsetEmpty(t *testing.T) {
	for _, names := range [][]string{nil, {"answer"}, {"id", "name"}} {
		for _, values := range [][][]any{nil, {}} {
			r, err := BuildSimpleBinaryResultset(names, values)
			require.NoError(t, err)
			require.Len(t, r.Fields, len(names))
			require.Empty(t, r.RowDatas)
			for i, name := range names {
				require.NotNil(t, r.Fields[i])
				require.Equal(t, name, string(r.Fields[i].Name))
				require.Equal(t, uint16(33), r.Fields[i].Charset)
				require.Equal(t, MYSQL_TYPE_NULL, r.Fields[i].Type)
			}
			(&Result{Resultset: r}).Close()
		}
	}
}

func TestFormatTextValueEmptyString(t *testing.T) {
	strBytes, err := FormatTextValue("")
	require.NoError(t, err)
	require.NotNil(t, strBytes)
	require.Empty(t, strBytes)

	sliceBytes, err := FormatTextValue([]byte{})
	require.NoError(t, err)
	require.NotNil(t, sliceBytes)
	require.Empty(t, sliceBytes)

	require.Equal(t, strBytes, sliceBytes)

	nilBytes, err := FormatTextValue(nil)
	require.NoError(t, err)
	require.Nil(t, nilBytes)
}

func TestBuildSimpleTextResultsetEmptyStringParity(t *testing.T) {
	r, err := BuildSimpleTextResultset(
		[]string{"empty_str", "empty_bytes", "null_val"},
		[][]any{{"", []byte{}, nil}},
	)
	require.NoError(t, err)
	require.Equal(t, RowData{0x00, 0x00, 0xfb}, r.RowDatas[0])
	require.NotEqual(t, byte(0xfb), r.RowDatas[0][0])
	require.Equal(t, r.RowDatas[0][0], r.RowDatas[0][1])
	require.Equal(t, byte(0xfb), r.RowDatas[0][2])
}
