package mysql

import (
	"database/sql"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// encodeBinaryRow assembles a binary-protocol row (header + NULL bitmap +
// payload) using EncodeBinaryFieldValue. Mirrors the production row
// builders, including the (nil, nil) NULL signal.
func encodeBinaryRow(t *testing.T, fields []*Field, values []any) []byte {
	t.Helper()
	require.Equal(t, len(fields), len(values), "fields/values length mismatch")
	bitmapLen := (len(fields) + 7 + 2) >> 3
	nullBitmap := make([]byte, bitmapLen)
	var payload []byte
	for i, v := range values {
		b, err := EncodeBinaryFieldValue(fields[i], v)
		require.NoError(t, err, "field %d (%s)", i, fields[i].Name)
		if b == nil {
			nullBitmap[(i+2)/8] |= 1 << (uint(i+2) % 8)
			continue
		}
		payload = append(payload, b...)
	}
	row := []byte{0x00}
	row = append(row, nullBitmap...)
	row = append(row, payload...)
	return row
}

// roundTrip encodes a row then parses it back.
func roundTrip(t *testing.T, fields []*Field, values []any) []FieldValue {
	t.Helper()
	raw := encodeBinaryRow(t, fields, values)
	parsed, err := RowData(raw).ParseBinary(fields, nil)
	require.NoError(t, err)
	return parsed
}

func TestEncodeBinaryFieldValueIntegers(t *testing.T) {
	t.Run("widths and signedness", func(t *testing.T) {
		fields := []*Field{
			{Name: []byte("c_tiny"), Type: MYSQL_TYPE_TINY},
			{Name: []byte("c_utiny"), Type: MYSQL_TYPE_TINY, Flag: UNSIGNED_FLAG},
			{Name: []byte("c_short"), Type: MYSQL_TYPE_SHORT},
			{Name: []byte("c_year"), Type: MYSQL_TYPE_YEAR, Flag: UNSIGNED_FLAG},
			{Name: []byte("c_int24"), Type: MYSQL_TYPE_INT24},
			{Name: []byte("c_long"), Type: MYSQL_TYPE_LONG},
			{Name: []byte("c_ulong"), Type: MYSQL_TYPE_LONG, Flag: UNSIGNED_FLAG},
			{Name: []byte("c_ll"), Type: MYSQL_TYPE_LONGLONG},
			{Name: []byte("c_ull"), Type: MYSQL_TYPE_LONGLONG, Flag: UNSIGNED_FLAG},
		}
		row := roundTrip(t, fields, []any{
			int8(-7), uint8(200),
			int16(-12345), uint16(2026),
			int32(-1234567), int32(2147483600), uint32(4294967290),
			int64(math.MinInt64), uint64(math.MaxUint64),
		})
		require.Equal(t, int64(-7), row[0].AsInt64())
		require.Equal(t, uint64(200), row[1].AsUint64())
		require.Equal(t, int64(-12345), row[2].AsInt64())
		require.Equal(t, uint64(2026), row[3].AsUint64())
		require.Equal(t, int64(-1234567), row[4].AsInt64())
		require.Equal(t, int64(2147483600), row[5].AsInt64())
		require.Equal(t, uint64(4294967290), row[6].AsUint64())
		require.Equal(t, int64(math.MinInt64), row[7].AsInt64())
		require.Equal(t, uint64(math.MaxUint64), row[8].AsUint64())
	})

	t.Run("signed range errors", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_TINY}
		_, err := EncodeBinaryFieldValue(f, int64(500))
		require.Error(t, err)
		_, err = EncodeBinaryFieldValue(f, int64(-500))
		require.Error(t, err)
	})

	t.Run("unsigned rejects negative", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_TINY, Flag: UNSIGNED_FLAG}
		_, err := EncodeBinaryFieldValue(f, int64(-1))
		require.Error(t, err)
	})

	t.Run("unsigned range errors", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_SHORT, Flag: UNSIGNED_FLAG}
		_, err := EncodeBinaryFieldValue(f, uint64(70000))
		require.Error(t, err)
	})

	t.Run("INT24 boundary", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_INT24}
		_, err := EncodeBinaryFieldValue(f, int64(1<<23-1))
		require.NoError(t, err)
		_, err = EncodeBinaryFieldValue(f, int64(1<<23))
		require.Error(t, err)
	})

	t.Run("YEAR forced unsigned without UNSIGNED_FLAG", func(t *testing.T) {
		// YEAR is always unsigned, even if the caller forgot UNSIGNED_FLAG.
		fields := []*Field{{Name: []byte("c_year"), Type: MYSQL_TYPE_YEAR}}
		row := roundTrip(t, fields, []any{uint16(2155)})
		require.Equal(t, uint64(2155), row[0].AsUint64())
		_, err := EncodeBinaryFieldValue(fields[0], int16(-1))
		require.Error(t, err)
	})
}

func TestEncodeBinaryFieldValueFloats(t *testing.T) {
	t.Run("FLOAT round-trip", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_FLOAT}}
		row := roundTrip(t, fields, []any{float32(3.5)})
		require.InDelta(t, 3.5, row[0].AsFloat64(), 1e-6)
	})

	t.Run("FLOAT rejects finite input that overflows to ±Inf", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_FLOAT}
		_, err := EncodeBinaryFieldValue(f, float64(1e40))
		require.Error(t, err)
		_, err = EncodeBinaryFieldValue(f, float64(-1e40))
		require.Error(t, err)
	})

	t.Run("FLOAT preserves caller-supplied Inf and NaN", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_FLOAT}
		_, err := EncodeBinaryFieldValue(f, math.Inf(1))
		require.NoError(t, err)
		_, err = EncodeBinaryFieldValue(f, math.Inf(-1))
		require.NoError(t, err)
		_, err = EncodeBinaryFieldValue(f, math.NaN())
		require.NoError(t, err)
	})

	t.Run("DOUBLE round-trip", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_DOUBLE}}
		row := roundTrip(t, fields, []any{float64(2.718281828459045)})
		require.InDelta(t, 2.718281828459045, row[0].AsFloat64(), 1e-12)
	})

	t.Run("DOUBLE accepts integer input as float", func(t *testing.T) {
		// Regression: FormatBinaryValue used to return int bits, which
		// reinterpret as float64 garbage.
		fields := []*Field{{Type: MYSQL_TYPE_DOUBLE}}
		row := roundTrip(t, fields, []any{int64(42)})
		require.InDelta(t, 42.0, row[0].AsFloat64(), 1e-12)
	})
}

func TestEncodeBinaryFieldValueTemporals(t *testing.T) {
	t.Run("DATE round-trip from string", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_DATE}}
		row := roundTrip(t, fields, []any{"2026-04-28"})
		require.Equal(t, "2026-04-28", string(row[0].AsString()))
	})

	t.Run("DATE zero sentinel", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_DATE}}
		row := roundTrip(t, fields, []any{"0000-00-00"})
		require.Equal(t, "0000-00-00", string(row[0].AsString()))
	})

	t.Run("DATETIME round-trip from string with fractional seconds", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_DATETIME}}
		row := roundTrip(t, fields, []any{"2026-04-28 09:30:15.123456"})
		require.Equal(t, "2026-04-28 09:30:15.123456", string(row[0].AsString()))
	})

	t.Run("DATETIME date-only string", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_DATETIME}}
		row := roundTrip(t, fields, []any{"2026-04-28"})
		require.Equal(t, "2026-04-28 00:00:00", string(row[0].AsString()))
	})

	t.Run("DATETIME zero sentinel", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_DATETIME}}
		row := roundTrip(t, fields, []any{"0000-00-00 00:00:00"})
		require.Equal(t, "0000-00-00 00:00:00", string(row[0].AsString()))
	})

	t.Run("TIMESTAMP round-trip", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIMESTAMP}}
		row := roundTrip(t, fields, []any{"2026-04-28 09:30:15"})
		require.Equal(t, "2026-04-28 09:30:15", string(row[0].AsString()))
	})

	t.Run("TIME round-trip", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{"12:34:56"})
		require.Equal(t, "12:34:56", string(row[0].AsString()))
	})

	t.Run("TIME negative", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{"-12:34:56"})
		require.Equal(t, "-12:34:56", string(row[0].AsString()))
	})

	t.Run("TIME large hour (>24)", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{"100:00:00"})
		require.Equal(t, "100:00:00", string(row[0].AsString()))
	})

	t.Run("TIME with fractional seconds", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{"12:34:56.123456"})
		require.Equal(t, "12:34:56.123456", string(row[0].AsString()))
	})

	t.Run("TIME zero sentinel", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{"00:00:00"})
		require.Equal(t, "00:00:00", string(row[0].AsString()))
	})

	t.Run("TIME bytes input from parser", func(t *testing.T) {
		// FieldValue.Value() returns []byte for TIME columns.
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{[]byte("13:45:00")})
		require.Equal(t, "13:45:00", string(row[0].AsString()))
	})

	t.Run("invalid DATE string errors", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_DATE}
		_, err := EncodeBinaryFieldValue(f, "not-a-date")
		require.Error(t, err)
	})

	t.Run("invalid TIME string errors", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_TIME}
		_, err := EncodeBinaryFieldValue(f, "12:34")
		require.Error(t, err)
	})
}

func TestEncodeBinaryFieldValueNullables(t *testing.T) {
	t.Run("sql.NullString valid", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_VARCHAR}}
		row := roundTrip(t, fields, []any{sql.NullString{String: "hi", Valid: true}})
		require.Equal(t, "hi", string(row[0].AsString()))
	})

	t.Run("sql.NullString invalid encodes as nil (NULL bitmap)", func(t *testing.T) {
		// Encoder returns (nil, nil) for an invalid Null wrapper; row
		// builders set the NULL bit and write no payload.
		f := &Field{Type: MYSQL_TYPE_VARCHAR}
		b, err := EncodeBinaryFieldValue(f, sql.NullString{Valid: false})
		require.NoError(t, err)
		require.Nil(t, b)
	})

	t.Run("sql.NullInt64 valid", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_LONGLONG}}
		row := roundTrip(t, fields, []any{sql.NullInt64{Int64: 42, Valid: true}})
		require.Equal(t, int64(42), row[0].AsInt64())
	})

	t.Run("sql.NullFloat64 valid", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_DOUBLE}}
		row := roundTrip(t, fields, []any{sql.NullFloat64{Float64: 3.14, Valid: true}})
		require.InDelta(t, 3.14, row[0].AsFloat64(), 1e-9)
	})
}

// TestEncodeBinaryFieldValueNullableRowAlignment regresses the NULL-bitmap
// alignment bug: a Null wrapper with Valid:false is non-nil, so a row
// builder that only checks `v == nil` skips the bit and shifts subsequent
// columns by one byte. The fix routes literal nil, driver.Valuer→nil, and
// MYSQL_TYPE_NULL through a single (nil, nil) signal.
func TestEncodeBinaryFieldValueNullableRowAlignment(t *testing.T) {
	t.Run("sql.NullString Valid:false followed by non-NULL column", func(t *testing.T) {
		fields := []*Field{
			{Name: []byte("c_first"), Type: MYSQL_TYPE_VARCHAR},
			{Name: []byte("c_second"), Type: MYSQL_TYPE_VARCHAR},
		}
		row := roundTrip(t, fields, []any{
			sql.NullString{Valid: false},
			"after",
		})
		require.Nil(t, row[0].Value(), "column 0 must be NULL, not shifted")
		require.Equal(t, "after", string(row[1].AsString()),
			"column 1 must round-trip intact, not consume column 0's bytes")
	})

	t.Run("sql.NullInt64 Valid:false followed by non-NULL column", func(t *testing.T) {
		fields := []*Field{
			{Name: []byte("c_first"), Type: MYSQL_TYPE_LONGLONG},
			{Name: []byte("c_second"), Type: MYSQL_TYPE_VARCHAR},
		}
		row := roundTrip(t, fields, []any{
			sql.NullInt64{Valid: false},
			"after",
		})
		require.Nil(t, row[0].Value())
		require.Equal(t, "after", string(row[1].AsString()))
	})

	t.Run("MYSQL_TYPE_NULL column followed by non-NULL column", func(t *testing.T) {
		fields := []*Field{
			{Name: []byte("c_null"), Type: MYSQL_TYPE_NULL},
			{Name: []byte("c_second"), Type: MYSQL_TYPE_VARCHAR},
		}
		row := roundTrip(t, fields, []any{
			"ignored",
			"after",
		})
		require.Nil(t, row[0].Value())
		require.Equal(t, "after", string(row[1].AsString()))
	})
}

// TestEncodeBinaryFieldValueProxyRoundTrip simulates the proxy flow: parse
// an upstream row, take Value() per column, encode again, parse again, and
// check the values match. Pre-fix, Value() returned ASCII for DATE/DATETIME/
// TIME and the encoder length-encoded that ASCII, producing unparseable bytes.
func TestEncodeBinaryFieldValueProxyRoundTrip(t *testing.T) {
	fields := []*Field{
		{Name: []byte("c_tiny"), Type: MYSQL_TYPE_TINY},
		{Name: []byte("c_short"), Type: MYSQL_TYPE_SHORT, Flag: UNSIGNED_FLAG},
		{Name: []byte("c_long"), Type: MYSQL_TYPE_LONG},
		{Name: []byte("c_ll"), Type: MYSQL_TYPE_LONGLONG, Flag: UNSIGNED_FLAG},
		{Name: []byte("c_float"), Type: MYSQL_TYPE_FLOAT},
		{Name: []byte("c_double"), Type: MYSQL_TYPE_DOUBLE},
		{Name: []byte("c_varchar"), Type: MYSQL_TYPE_VARCHAR},
		{Name: []byte("c_blob"), Type: MYSQL_TYPE_BLOB},
		{Name: []byte("c_decimal"), Type: MYSQL_TYPE_NEWDECIMAL},
		{Name: []byte("c_json"), Type: MYSQL_TYPE_JSON},
		{Name: []byte("c_date"), Type: MYSQL_TYPE_DATE},
		{Name: []byte("c_datetime"), Type: MYSQL_TYPE_DATETIME},
		{Name: []byte("c_timestamp"), Type: MYSQL_TYPE_TIMESTAMP},
		{Name: []byte("c_time"), Type: MYSQL_TYPE_TIME},
	}
	original := []any{
		int8(-7),
		uint16(2026),
		int32(-1234567),
		uint64(9223372036854775000),
		float32(3.5),
		float64(2.718281828459045),
		"hello",
		[]byte{0x00, 0x01, 0x02},
		"1234.5678",
		[]byte(`{"k":"v"}`),
		"2026-04-28",
		"2026-04-28 09:30:15.123456",
		"2026-04-28 09:30:15",
		"-100:34:56",
	}

	// First leg: encode original → parse → take Value() per field.
	parsed1 := roundTrip(t, fields, original)
	intermediate := make([]any, len(fields))
	for i, fv := range parsed1 {
		intermediate[i] = fv.Value()
	}

	// Second leg: encode the intermediate values → parse again.
	parsed2 := roundTrip(t, fields, intermediate)

	// Both passes must agree.
	for i := range fields {
		require.Equal(t, parsed1[i].Value(), parsed2[i].Value(),
			"column %d (%s) diverged on round-trip", i, fields[i].Name)
	}
}

// TestEncodeBinaryFieldValueBool exercises the bool path used by sql.NullBool
// and bare bool values destined for TINYINT(1) (MySQL's "BOOL" alias).
func TestEncodeBinaryFieldValueBool(t *testing.T) {
	t.Run("bare bool true → 1", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TINY, Flag: UNSIGNED_FLAG}}
		row := roundTrip(t, fields, []any{true})
		require.Equal(t, uint64(1), row[0].AsUint64())
	})

	t.Run("bare bool false → 0", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TINY, Flag: UNSIGNED_FLAG}}
		row := roundTrip(t, fields, []any{false})
		require.Equal(t, uint64(0), row[0].AsUint64())
	})

	t.Run("sql.NullBool valid:true", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TINY, Flag: UNSIGNED_FLAG}}
		row := roundTrip(t, fields, []any{sql.NullBool{Bool: true, Valid: true}})
		require.Equal(t, uint64(1), row[0].AsUint64())
	})

	t.Run("sql.NullBool valid:false → NULL", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TINY, Flag: UNSIGNED_FLAG}}
		row := roundTrip(t, fields, []any{sql.NullBool{Valid: false}})
		require.Nil(t, row[0].Value())
	})

	t.Run("bool rejected for FLOAT", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_FLOAT}
		_, err := EncodeBinaryFieldValue(f, true)
		require.Error(t, err)
	})

	t.Run("bool rejected for non-TINY integer types", func(t *testing.T) {
		// MySQL BOOL is strictly TINYINT(1); accepting bool for wider int
		// types would silently paper over caller bugs.
		for _, fieldType := range []byte{
			MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG,
		} {
			f := &Field{Type: fieldType}
			_, err := EncodeBinaryFieldValue(f, true)
			require.Error(t, err, "bool must be rejected for column type %d", fieldType)
		}
	})
}

func TestEncodeBinaryFieldValueValidationErrors(t *testing.T) {
	t.Run("DATETIME with trailing whitespace and no time part rejects", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_DATETIME}
		_, err := EncodeBinaryFieldValue(f, "2026-04-28 ")
		require.Error(t, err)
	})

	t.Run("DATE max 4-digit year accepted, 5-digit string rejected at format check", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_DATE}
		_, err := EncodeBinaryFieldValue(f, "9999-01-01")
		require.NoError(t, err)
		// "10000-01-01" is len=11, failing the strict YYYY-MM-DD format check.
		_, err = EncodeBinaryFieldValue(f, "10000-01-01")
		require.Error(t, err)
	})

	t.Run("TIME with >6 fractional digits rejects", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_TIME}
		_, err := EncodeBinaryFieldValue(f, "12:34:56.1234567")
		require.Error(t, err)
	})

	t.Run("TIME with hours > 838 rejects", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_TIME}
		_, err := EncodeBinaryFieldValue(f, "839:00:00")
		require.Error(t, err)
		_, err = EncodeBinaryFieldValue(f, "838:59:59")
		require.NoError(t, err)
	})

	t.Run("TIME at the absolute maximum 838:59:59.999999 round-trips", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{"838:59:59.999999"})
		require.Equal(t, "838:59:59.999999", string(row[0].AsString()))
	})

	t.Run("DATETIME with invalid time-part rejects", func(t *testing.T) {
		f := &Field{Type: MYSQL_TYPE_DATETIME}
		_, err := EncodeBinaryFieldValue(f, "2026-04-28 25:00:00")
		require.Error(t, err)
	})

	t.Run("trailing-dot fraction rejected for TIME and DATETIME", func(t *testing.T) {
		fTime := &Field{Type: MYSQL_TYPE_TIME}
		_, err := EncodeBinaryFieldValue(fTime, "12:34:56.")
		require.Error(t, err)

		fDt := &Field{Type: MYSQL_TYPE_DATETIME}
		_, err = EncodeBinaryFieldValue(fDt, "2026-04-28 12:34:56.")
		require.Error(t, err)
	})

	t.Run("empty string rejected for DATE / DATETIME / TIME", func(t *testing.T) {
		for _, fieldType := range []byte{
			MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_TIME,
		} {
			f := &Field{Type: fieldType}
			_, err := EncodeBinaryFieldValue(f, "")
			require.Error(t, err, "empty string must be rejected for column type %d", fieldType)
		}
	})

	t.Run("negative all-zero TIME collapses to zero sentinel", func(t *testing.T) {
		fields := []*Field{{Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{"-00:00:00"})
		// The wire format has no negative-zero TIME.
		require.Equal(t, "00:00:00", string(row[0].AsString()))
	})

	t.Run("unsupported field type errors", func(t *testing.T) {
		// TIMESTAMP2 is binlog-internal, not valid on the resultset wire.
		f := &Field{Type: MYSQL_TYPE_TIMESTAMP2}
		_, err := EncodeBinaryFieldValue(f, "2026-04-28 09:30:15")
		require.Error(t, err)
	})

	t.Run("nil *Field rejected, not panicked", func(t *testing.T) {
		_, err := EncodeBinaryFieldValue(nil, int64(1))
		require.Error(t, err)
	})
}

// TestEncodeBinaryFieldValueTypedNilSlice verifies a typed-nil []byte
// (e.g. var b []byte) encodes as NULL, not an empty length-encoded string.
// The interface holding the typed nil is itself non-nil, so the top-level
// nil check misses it; each []byte branch handles it explicitly.
func TestEncodeBinaryFieldValueTypedNilSlice(t *testing.T) {
	var b []byte

	t.Run("VARCHAR typed-nil []byte → NULL, next column intact", func(t *testing.T) {
		fields := []*Field{
			{Name: []byte("c_first"), Type: MYSQL_TYPE_VARCHAR},
			{Name: []byte("c_second"), Type: MYSQL_TYPE_VARCHAR},
		}
		row := roundTrip(t, fields, []any{b, "after"})
		require.Nil(t, row[0].Value(), "typed-nil []byte must encode as NULL")
		require.Equal(t, "after", string(row[1].AsString()))
	})

	t.Run("DATE typed-nil []byte → NULL", func(t *testing.T) {
		fields := []*Field{{Name: []byte("c_date"), Type: MYSQL_TYPE_DATE}}
		row := roundTrip(t, fields, []any{b})
		require.Nil(t, row[0].Value())
	})

	t.Run("DATETIME typed-nil []byte → NULL", func(t *testing.T) {
		fields := []*Field{{Name: []byte("c_dt"), Type: MYSQL_TYPE_DATETIME}}
		row := roundTrip(t, fields, []any{b})
		require.Nil(t, row[0].Value())
	})

	t.Run("TIME typed-nil []byte → NULL", func(t *testing.T) {
		fields := []*Field{{Name: []byte("c_time"), Type: MYSQL_TYPE_TIME}}
		row := roundTrip(t, fields, []any{b})
		require.Nil(t, row[0].Value())
	})

	t.Run("empty non-nil []byte still aligns the wire (next column intact)", func(t *testing.T) {
		// FieldValue's parser collapses a zero-length string back to a
		// typed-nil slice, so .Value() can't distinguish empty from NULL.
		// We instead verify wire alignment: column 1 round-trips intact
		// only if column 0 wrote a length=0 prefix, which is what
		// differentiates empty from NULL on the wire.
		fields := []*Field{
			{Name: []byte("c_first"), Type: MYSQL_TYPE_VARCHAR},
			{Name: []byte("c_second"), Type: MYSQL_TYPE_VARCHAR},
		}
		row := roundTrip(t, fields, []any{[]byte{}, "after"})
		require.Equal(t, "after", string(row[1].AsString()))
	})
}
