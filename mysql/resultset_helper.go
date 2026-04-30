package mysql

import (
	"database/sql/driver"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/utils"
)

func FormatTextValue(value any) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case uint8:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(nil, v, 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case float32:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.AppendFloat(nil, v, 'f', -1, 64), nil
	case []byte:
		return v, nil
	case string:
		return utils.StringToByteSlice(v), nil
	case time.Time:
		return utils.StringToByteSlice(v.Format(time.DateTime)), nil
	case nil:
		return nil, nil
	default:
		return nil, errors.Errorf("invalid type %T", value)
	}
}

// packDate emits the wire bytes for a DATE.
//
//	length | payload
//	-------+------------------------
//	     0 | (zero sentinel)
//	     4 | year(2) month(1) day(1)
func packDate(year uint16, month, day uint8) []byte {
	if year == 0 && month == 0 && day == 0 {
		return []byte{0}
	}
	return []byte{4, byte(year), byte(year >> 8), month, day}
}

// packDateTime emits the wire bytes for a DATETIME / TIMESTAMP. The length
// shortens when trailing fields (time, then microseconds) are zero.
//
//	length | payload
//	-------+-----------------------------------------------------------
//	     0 | (zero sentinel)
//	     4 | year(2) month(1) day(1)
//	     7 | year(2) month(1) day(1) hour(1) min(1) sec(1)
//	    11 | year(2) month(1) day(1) hour(1) min(1) sec(1) micro(4)
func packDateTime(year uint16, month, day, hour, minute, sec uint8, micro uint32) []byte {
	if year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && sec == 0 && micro == 0 {
		return []byte{0}
	}
	if micro > 0 {
		return []byte{
			11,
			byte(year), byte(year >> 8), month, day,
			hour, minute, sec,
			byte(micro), byte(micro >> 8), byte(micro >> 16), byte(micro >> 24),
		}
	}
	if hour > 0 || minute > 0 || sec > 0 {
		return []byte{
			7,
			byte(year), byte(year >> 8), month, day,
			hour, minute, sec,
		}
	}
	return []byte{
		4,
		byte(year), byte(year >> 8), month, day,
	}
}

// packTime emits the wire bytes for a TIME. MySQL TIME accepts hours up to
// 838, expressed on the wire as (days*24 + hour). An all-zero TIME collapses
// to the zero sentinel regardless of sign.
//
//	length | payload
//	-------+--------------------------------------------------
//	     0 | (zero sentinel)
//	     8 | neg(1) days(4) hour(1) min(1) sec(1)
//	    12 | neg(1) days(4) hour(1) min(1) sec(1) micro(4)
func packTime(negative bool, days uint32, hour, minute, sec uint8, micro uint32) []byte {
	if days == 0 && hour == 0 && minute == 0 && sec == 0 && micro == 0 {
		return []byte{0}
	}
	var negByte byte
	if negative {
		negByte = 1
	}
	if micro > 0 {
		return []byte{
			12,
			negByte,
			byte(days), byte(days >> 8), byte(days >> 16), byte(days >> 24),
			hour, minute, sec,
			byte(micro), byte(micro >> 8), byte(micro >> 16), byte(micro >> 24),
		}
	}
	return []byte{
		8,
		negByte,
		byte(days), byte(days >> 8), byte(days >> 16), byte(days >> 24),
		hour, minute, sec,
	}
}

// toBinaryDate encodes a time.Time as a length-prefixed packed binary DATE.
func toBinaryDate(t time.Time) ([]byte, error) {
	if t.IsZero() {
		return []byte{0}, nil
	}
	return packDate(uint16(t.Year()), uint8(t.Month()), uint8(t.Day())), nil
}

// toBinaryTime encodes a time.Time as a length-prefixed packed binary TIME.
// For negative or >23h values, pass a string instead.
func toBinaryTime(t time.Time) ([]byte, error) {
	if t.IsZero() {
		return []byte{0}, nil
	}
	return packTime(
		false, 0,
		uint8(t.Hour()), uint8(t.Minute()), uint8(t.Second()),
		uint32(t.Nanosecond()/1000),
	), nil
}

// toBinaryDateTime encodes a time.Time as a length-prefixed packed binary
// DATETIME / TIMESTAMP.
func toBinaryDateTime(t time.Time) ([]byte, error) {
	if t.IsZero() {
		return []byte{0}, nil
	}
	return packDateTime(
		uint16(t.Year()), uint8(t.Month()), uint8(t.Day()),
		uint8(t.Hour()), uint8(t.Minute()), uint8(t.Second()),
		uint32(t.Nanosecond()/1000),
	), nil
}

// parseDateString re-packs a "YYYY-MM-DD" DATE string (the form produced
// by ParseBinary) into the wire's packed format. Empty input is rejected.
func parseDateString(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.Errorf("invalid DATE: empty string")
	}
	if s == "0000-00-00" {
		return []byte{0}, nil
	}
	year, month, day, err := parseYMD(s)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid DATE %q", s)
	}
	return packDate(year, month, day), nil
}

// parseDateTimeString re-packs a "YYYY-MM-DD[ HH:MM:SS[.ffffff]]"
// DATETIME / TIMESTAMP string into the wire's packed format.
func parseDateTimeString(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.Errorf("invalid DATETIME: empty string")
	}
	// Fast-path the canonical zero sentinels. Other zero-fraction widths
	// fall through to packDateTime, which collapses all-zero values to []byte{0}.
	if s == "0000-00-00 00:00:00" || s == "0000-00-00 00:00:00.000000" {
		return []byte{0}, nil
	}
	datePart, timePart, hasSpace := strings.Cut(s, " ")
	if hasSpace && timePart == "" {
		// Reject "YYYY-MM-DD " with trailing whitespace; otherwise we
		// silently emit a date-only encoding for malformed input.
		return nil, errors.Errorf("invalid DATETIME %q: trailing whitespace without time part", s)
	}
	year, month, day, err := parseYMD(datePart)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid DATETIME %q", s)
	}
	var hour, minute, sec uint8
	var micro uint32
	if timePart != "" {
		hour, minute, sec, micro, err = parseHMSFraction(timePart)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid DATETIME %q", s)
		}
	}
	return packDateTime(year, month, day, hour, minute, sec, micro), nil
}

// parseTimeString re-packs a "[-]H+:MM:SS[.ffffff]" TIME string into the
// wire's packed format. Hours may exceed 23 (up to 838) and are split into
// days plus an in-day hour.
func parseTimeString(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.Errorf("invalid TIME: empty string")
	}
	// Fast-path the canonical zero sentinels. packTime collapses any all-zero
	// value to []byte{0}, so "-00:00:00" still produces the right wire bytes.
	if s == "00:00:00" || s == "00:00:00.000000" {
		return []byte{0}, nil
	}
	negative := strings.HasPrefix(s, "-")
	if negative {
		s = s[1:]
	}
	hms, frac, hasDot := strings.Cut(s, ".")
	if hasDot && frac == "" {
		return nil, errors.Errorf("invalid TIME %q: trailing dot without fractional digits", s)
	}
	parts := strings.Split(hms, ":")
	if len(parts) != 3 {
		return nil, errors.Errorf("invalid TIME %q", s)
	}
	totalHours, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid TIME %q", s)
	}
	if totalHours > 838 {
		// MySQL TIME range is -838:59:59 to 838:59:59.
		return nil, errors.Errorf("invalid TIME %q: hours %d exceed MySQL TIME maximum of 838", s, totalHours)
	}
	minute, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid TIME %q: minute", s)
	}
	if minute > 59 {
		return nil, errors.Errorf("invalid TIME %q: minute %d out of range", s, minute)
	}
	sec, err := strconv.ParseUint(parts[2], 10, 8)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid TIME %q: second", s)
	}
	if sec > 59 {
		return nil, errors.Errorf("invalid TIME %q: second %d out of range", s, sec)
	}
	var micro uint32
	if frac != "" {
		micro, err = parseMicroseconds(frac)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid TIME %q", s)
		}
	}
	days := uint32(totalHours / 24)
	hour := uint8(totalHours % 24)
	return packTime(negative, days, hour, uint8(minute), uint8(sec), micro), nil
}

func parseYMD(s string) (uint16, uint8, uint8, error) {
	if len(s) != 10 || s[4] != '-' || s[7] != '-' {
		return 0, 0, 0, errors.Errorf("expected YYYY-MM-DD")
	}
	year, err := strconv.ParseUint(s[0:4], 10, 16)
	if err != nil {
		return 0, 0, 0, err
	}
	month, err := strconv.ParseUint(s[5:7], 10, 8)
	if err != nil || month > 12 {
		return 0, 0, 0, errors.Errorf("month out of range")
	}
	day, err := strconv.ParseUint(s[8:10], 10, 8)
	if err != nil || day > 31 {
		return 0, 0, 0, errors.Errorf("day out of range")
	}
	return uint16(year), uint8(month), uint8(day), nil
}

func parseHMSFraction(s string) (uint8, uint8, uint8, uint32, error) {
	hms, frac, hasDot := strings.Cut(s, ".")
	if hasDot && frac == "" {
		return 0, 0, 0, 0, errors.Errorf("trailing dot without fractional digits")
	}
	parts := strings.Split(hms, ":")
	if len(parts) != 3 {
		return 0, 0, 0, 0, errors.Errorf("expected HH:MM:SS")
	}
	hour, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil {
		return 0, 0, 0, 0, errors.Wrap(err, "hour")
	}
	if hour > 23 {
		return 0, 0, 0, 0, errors.Errorf("hour %d out of range", hour)
	}
	minute, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return 0, 0, 0, 0, errors.Wrap(err, "minute")
	}
	if minute > 59 {
		return 0, 0, 0, 0, errors.Errorf("minute %d out of range", minute)
	}
	sec, err := strconv.ParseUint(parts[2], 10, 8)
	if err != nil {
		return 0, 0, 0, 0, errors.Wrap(err, "second")
	}
	if sec > 59 {
		return 0, 0, 0, 0, errors.Errorf("second %d out of range", sec)
	}
	var micro uint32
	if frac != "" {
		micro, err = parseMicroseconds(frac)
		if err != nil {
			return 0, 0, 0, 0, err
		}
	}
	return uint8(hour), uint8(minute), uint8(sec), micro, nil
}

func parseMicroseconds(s string) (uint32, error) {
	if len(s) > 6 {
		// MySQL fractional seconds top out at 6 digits.
		return 0, errors.Errorf("invalid microseconds %q: more than 6 digits", s)
	}
	for len(s) < 6 {
		s += "0"
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid microseconds %q", s)
	}
	return uint32(n), nil
}

// unwrapDriverValue unwraps database/sql driver wrappers (sql.NullString,
// sql.NullInt64, anything implementing driver.Valuer) before encoding.
// One level only, matching stdlib's database/sql/driver behavior.
func unwrapDriverValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if v, ok := value.(driver.Valuer); ok {
		return v.Value()
	}
	return value, nil
}

func coerceInt(value any) (int64, error) {
	switch v := value.(type) {
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return 0, errors.Errorf("uint %d overflows int64", v)
		}
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, errors.Errorf("uint64 %d overflows int64", v)
		}
		return int64(v), nil
	default:
		return 0, errors.Errorf("cannot coerce %T to integer", value)
	}
}

func coerceUint(value any) (uint64, error) {
	switch v := value.(type) {
	case int8:
		if v < 0 {
			return 0, errors.Errorf("negative value %d for unsigned column", v)
		}
		return uint64(v), nil
	case int16:
		if v < 0 {
			return 0, errors.Errorf("negative value %d for unsigned column", v)
		}
		return uint64(v), nil
	case int32:
		if v < 0 {
			return 0, errors.Errorf("negative value %d for unsigned column", v)
		}
		return uint64(v), nil
	case int:
		if v < 0 {
			return 0, errors.Errorf("negative value %d for unsigned column", v)
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, errors.Errorf("negative value %d for unsigned column", v)
		}
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint64:
		return v, nil
	default:
		return 0, errors.Errorf("cannot coerce %T to unsigned integer", value)
	}
}

// coerceFloat64 converts a numeric Go value to float64 for binary encoding.
// uint64 → float64 silently loses precision above 2^53; this matches MySQL.
func coerceFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return 0, errors.Errorf("cannot coerce %T to float", value)
	}
}

// encodeIntegerColumn writes value at the per-type fixed width, range-
// checking against the column's declared signedness.
func encodeIntegerColumn(field *Field, value any) ([]byte, error) {
	// MySQL BOOL / BOOLEAN are aliases for TINYINT(1); accept Go bool only
	// for TINY (encoded as 1/0) and reject for wider integer types.
	if b, ok := value.(bool); ok {
		if field.Type != MYSQL_TYPE_TINY {
			return nil, errors.Errorf("EncodeBinaryFieldValue: bool input not supported for column type %d (only TINY)", field.Type)
		}
		if b {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	}
	var width int
	var smin, smax int64
	var umax uint64
	switch field.Type {
	case MYSQL_TYPE_TINY:
		width, smin, smax, umax = 1, math.MinInt8, math.MaxInt8, math.MaxUint8
	case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
		// YEAR's wire bound is uint16; the 1901–2155 semantic range is the
		// server's concern, not enforced here.
		width, smin, smax, umax = 2, math.MinInt16, math.MaxInt16, math.MaxUint16
	case MYSQL_TYPE_INT24:
		// MEDIUMINT is 3 bytes on disk but 4 bytes on the wire.
		width, smin, smax, umax = 4, -1<<23, 1<<23-1, 1<<24-1
	case MYSQL_TYPE_LONG:
		width, smin, smax, umax = 4, math.MinInt32, math.MaxInt32, math.MaxUint32
	case MYSQL_TYPE_LONGLONG:
		width, smin, smax, umax = 8, math.MinInt64, math.MaxInt64, math.MaxUint64
	default:
		return nil, errors.Errorf("not an integer type: %d", field.Type)
	}
	// MySQL YEAR is always unsigned (range 1901–2155 plus sentinel 0000), so
	// force the unsigned path even if the caller forgot UNSIGNED_FLAG.
	unsigned := field.Flag&UNSIGNED_FLAG != 0 || field.Type == MYSQL_TYPE_YEAR
	if unsigned {
		u, err := coerceUint(value)
		if err != nil {
			return nil, errors.Wrapf(err, "EncodeBinaryFieldValue (type %d)", field.Type)
		}
		if u > umax {
			return nil, errors.Errorf("EncodeBinaryFieldValue (type %d): value %d exceeds unsigned max %d", field.Type, u, umax)
		}
		return Uint64ToBytes(u)[:width], nil
	}
	i, err := coerceInt(value)
	if err != nil {
		return nil, errors.Wrapf(err, "EncodeBinaryFieldValue (type %d)", field.Type)
	}
	if i < smin || i > smax {
		return nil, errors.Errorf("EncodeBinaryFieldValue (type %d): value %d out of range [%d, %d]", field.Type, i, smin, smax)
	}
	return Uint64ToBytes(uint64(i))[:width], nil
}

// EncodeBinaryFieldValue encodes a single value for the binary protocol,
// honoring the column's declared type. Range-checks integer values and
// re-packs ParseBinary's formatted temporal strings into packed form.
//
// (nil, nil) signals NULL: the caller sets the row's NULL bitmap bit and
// writes no payload. Emitted for literal nil, driver.Valuer→nil, and
// MYSQL_TYPE_NULL.
func EncodeBinaryFieldValue(field *Field, value any) ([]byte, error) {
	if field == nil {
		return nil, errors.Errorf("EncodeBinaryFieldValue: nil field")
	}
	if field.Type == MYSQL_TYPE_NULL {
		return nil, nil
	}
	value, err := unwrapDriverValue(value)
	if err != nil {
		return nil, errors.Wrap(err, "EncodeBinaryFieldValue")
	}
	if value == nil {
		return nil, nil
	}

	switch field.Type {
	case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR,
		MYSQL_TYPE_INT24, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG:
		return encodeIntegerColumn(field, value)

	case MYSQL_TYPE_FLOAT:
		f, err := coerceFloat64(value)
		if err != nil {
			return nil, errors.Wrap(err, "EncodeBinaryFieldValue (FLOAT)")
		}
		f32 := float32(f)
		// Reject finite inputs that overflow float32 to ±Inf; preserve
		// genuine ±Inf and NaN. Underflow narrows to 0, matching MySQL.
		if math.IsInf(float64(f32), 0) && !math.IsInf(f, 0) {
			return nil, errors.Errorf("EncodeBinaryFieldValue (FLOAT): value %g exceeds float32 range", f)
		}
		return Uint32ToBytes(math.Float32bits(f32)), nil

	case MYSQL_TYPE_DOUBLE:
		f, err := coerceFloat64(value)
		if err != nil {
			return nil, errors.Wrap(err, "EncodeBinaryFieldValue (DOUBLE)")
		}
		return Uint64ToBytes(math.Float64bits(f)), nil

	case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING,
		MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
		MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET,
		MYSQL_TYPE_VECTOR, MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_JSON:
		switch v := value.(type) {
		case []byte:
			if v == nil {
				// Typed-nil []byte → NULL. The top-level nil check doesn't
				// catch typed nils inside an interface.
				return nil, nil
			}
			return PutLengthEncodedString(v), nil
		case string:
			return PutLengthEncodedString(utils.StringToByteSlice(v)), nil
		default:
			return nil, errors.Errorf("EncodeBinaryFieldValue: unsupported value type %T for field type %d", value, field.Type)
		}

	case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
		switch v := value.(type) {
		case time.Time:
			return toBinaryDate(v)
		case []byte:
			if v == nil {
				return nil, nil
			}
			return parseDateString(utils.ByteSliceToString(v))
		case string:
			return parseDateString(v)
		default:
			return nil, errors.Errorf("EncodeBinaryFieldValue: unsupported value type %T for DATE", value)
		}

	case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
		switch v := value.(type) {
		case time.Time:
			return toBinaryDateTime(v)
		case []byte:
			if v == nil {
				return nil, nil
			}
			return parseDateTimeString(utils.ByteSliceToString(v))
		case string:
			return parseDateTimeString(v)
		default:
			return nil, errors.Errorf("EncodeBinaryFieldValue: unsupported value type %T for DATETIME/TIMESTAMP", value)
		}

	case MYSQL_TYPE_TIME:
		switch v := value.(type) {
		case time.Time:
			return toBinaryTime(v)
		case []byte:
			if v == nil {
				return nil, nil
			}
			return parseTimeString(utils.ByteSliceToString(v))
		case string:
			return parseTimeString(v)
		default:
			return nil, errors.Errorf("EncodeBinaryFieldValue: unsupported value type %T for TIME", value)
		}

	default:
		// TIMESTAMP2 / DATETIME2 / TIME2 are binlog-internal, not on the wire.
		return nil, errors.Errorf("EncodeBinaryFieldValue: unsupported field type %d", field.Type)
	}
}

// FormatBinaryValue formats a value for binary protocol.
//
// Deprecated: use EncodeBinaryFieldValue, which honors the column's declared
// field type and width. FormatBinaryValue always emits 8 bytes for any
// integer Go type, producing wire bytes the client cannot parse for
// narrow-width columns. Zero time.Time → (nil, nil) is preserved for
// backward compat.
func FormatBinaryValue(value any) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return Uint64ToBytes(uint64(v)), nil
	case int16:
		return Uint64ToBytes(uint64(v)), nil
	case int32:
		return Uint64ToBytes(uint64(v)), nil
	case int64:
		return Uint64ToBytes(uint64(v)), nil
	case int:
		return Uint64ToBytes(uint64(v)), nil
	case uint8:
		return Uint64ToBytes(uint64(v)), nil
	case uint16:
		return Uint64ToBytes(uint64(v)), nil
	case uint32:
		return Uint64ToBytes(uint64(v)), nil
	case uint64:
		return Uint64ToBytes(v), nil
	case uint:
		return Uint64ToBytes(uint64(v)), nil
	case float32:
		return Uint64ToBytes(math.Float64bits(float64(v))), nil
	case float64:
		return Uint64ToBytes(math.Float64bits(v)), nil
	case []byte:
		return v, nil
	case string:
		return utils.StringToByteSlice(v), nil
	case time.Time:
		if v.IsZero() {
			return nil, nil
		}
		return toBinaryDateTime(v)
	default:
		return nil, errors.Errorf("invalid type %T", value)
	}
}

func fieldType(value any) (typ uint8, err error) {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		typ = MYSQL_TYPE_LONGLONG
	case uint8, uint16, uint32, uint64, uint:
		typ = MYSQL_TYPE_LONGLONG
	case float32, float64:
		typ = MYSQL_TYPE_DOUBLE
	case string, []byte:
		typ = MYSQL_TYPE_VAR_STRING
	case time.Time:
		typ = MYSQL_TYPE_DATETIME
	case nil:
		typ = MYSQL_TYPE_NULL
	default:
		err = errors.Errorf("unsupport type %T for resultset", value)
	}
	return typ, err
}

func formatField(field *Field, value any) error {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		field.Charset = 63
		field.Flag = BINARY_FLAG | NOT_NULL_FLAG
	case uint8, uint16, uint32, uint64, uint:
		field.Charset = 63
		field.Flag = BINARY_FLAG | NOT_NULL_FLAG | UNSIGNED_FLAG
	case float32, float64:
		field.Charset = 63
		field.Flag = BINARY_FLAG | NOT_NULL_FLAG
	case string, []byte, time.Time:
		field.Charset = 33
	case nil:
		field.Charset = 33
	default:
		return errors.Errorf("unsupport type %T for resultset", value)
	}
	return nil
}

func BuildSimpleTextResultset(names []string, values [][]any) (*Resultset, error) {
	r := NewResultset(len(names))

	var b []byte

	if len(values) == 0 {
		for i, name := range names {
			r.Fields[i] = &Field{Name: utils.StringToByteSlice(name), Charset: 33, Type: MYSQL_TYPE_NULL}
		}
		return r, nil
	}

	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, errors.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		for j, value := range vs {
			typ, err := fieldType(value)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if r.Fields[j] == nil {
				r.Fields[j] = &Field{Name: utils.StringToByteSlice(names[j]), Type: typ}
				err = formatField(r.Fields[j], value)
				if err != nil {
					return nil, errors.Trace(err)
				}
			} else if typ != r.Fields[j].Type {
				// we got another type in the same column. in general, we treat it as an error, except
				// the case, when old value was null, and the new one isn't null, so we can update
				// type info for fields.
				oldIsNull, newIsNull := r.Fields[j].Type == MYSQL_TYPE_NULL, typ == MYSQL_TYPE_NULL
				if oldIsNull && !newIsNull { // old is null, new isn't, update type info.
					r.Fields[j].Type = typ
					err = formatField(r.Fields[j], value)
					if err != nil {
						return nil, errors.Trace(err)
					}
				} else if !oldIsNull && !newIsNull { // different non-null types, that's an error.
					return nil, errors.Errorf("row types aren't consistent")
				}
			}
			b, err = FormatTextValue(value)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if b == nil {
				// NULL value is encoded as 0xfb here (without additional info about length)
				row = append(row, 0xfb)
			} else {
				row = append(row, PutLengthEncodedString(b)...)
			}
		}

		r.RowDatas = append(r.RowDatas, row)
	}

	return r, nil
}

func BuildSimpleBinaryResultset(names []string, values [][]any) (*Resultset, error) {
	r := NewResultset(len(names))

	var b []byte

	bitmapLen := (len(names) + 7 + 2) >> 3

	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, errors.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		nullBitmap := make([]byte, bitmapLen)

		row = append(row, 0)
		row = append(row, nullBitmap...)

		for j, value := range vs {
			typ, err := fieldType(value)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if i == 0 {
				field := &Field{Type: typ}
				r.Fields[j] = field
				field.Name = utils.StringToByteSlice(names[j])

				if err = formatField(field, value); err != nil {
					return nil, errors.Trace(err)
				}
			}
			if value == nil {
				nullBitmap[(j+2)/8] |= 1 << (uint(j+2) % 8)
				continue
			}

			b, err = FormatBinaryValue(value)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if r.Fields[j].Type == MYSQL_TYPE_VAR_STRING {
				row = append(row, PutLengthEncodedString(b)...)
			} else {
				row = append(row, b...)
			}
		}

		copy(row[1:], nullBitmap)

		r.RowDatas = append(r.RowDatas, row)
	}

	return r, nil
}

func BuildSimpleResultset(names []string, values [][]any, binary bool) (*Resultset, error) {
	if binary {
		return BuildSimpleBinaryResultset(names, values)
	} else {
		return BuildSimpleTextResultset(names, values)
	}
}
