package replication

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// renderJSONAsMySQLText walks a MySQL JSON binary (JSONB) byte stream and
// emits a textual representation closely matching what MySQL produces from
// "SELECT json_col" / "CAST(json_col AS char)". It is used when
// RowsEvent.renderJSONAsMySQLText is true.
//
// The default decoder routes every JSONB value through an intermediate Go
// value (float64, shopspring/decimal, map[string]any, ...) and then
// json.Marshal. That path silently changes the JSON *type tag*:
//
//   - JSONB_DOUBLE 1.0 -> float64(1.0) -> json.Marshal -> "1" (INTEGER)
//   - JSONB_OPAQUE NEWDECIMAL "1.0" -> Go string "1.0" -> json.Marshal ->
//     "\"1.0\"" (STRING)
//
// Replaying either of those into a MySQL JSON column changes the stored
// binary representation. Callers feeding the decoded string back into a
// MySQL JSON column (binlog-based replication / online schema change
// tools) need the original type to survive the round-trip; this renderer
// preserves the type tag by emitting text directly from the JSONB byte
// stream without an intermediate Go-typed value.
//
// Known divergences from MySQL's exact textual form:
//
//   - Invalid UTF-8 bytes inside JSONB_STRING values are replaced with
//     U+FFFD; MySQL preserves the raw bytes.
//   - JSONB_OPAQUE values of an unrecognised inner type are rendered as
//     "opaque:type=N:<hex>"; MySQL emits "base64:typeN:<base64>".
//   - JSONB_DOUBLE values use Go's strconv shortest-round-trip formatting
//     (whole numbers keep ".0"); MySQL uses its own my_gcvt rules and may
//     differ in scientific-notation exponent padding for edge magnitudes.
//
// These are noted because consumers replaying the rendered text into a
// MySQL JSON column will see those values land as JSON STRING rather
// than the original JSONB type.
func renderJSONAsMySQLText(data []byte, ignoreDecodeErr bool) ([]byte, error) {
	r := &jsonMySQLTextRenderer{ignoreDecodeErr: ignoreDecodeErr}
	if len(data) < 1 {
		if ignoreDecodeErr {
			return []byte("null"), nil
		}
		return nil, errors.New("json binary data is empty")
	}
	r.renderValue(data[0], data[1:])
	if r.err != nil {
		if ignoreDecodeErr {
			// Return a valid JSON document (matches the legacy decoder's
			// behaviour of returning Go nil -> json.Marshal -> "null"
			// rather than leaking a half-written buffer).
			return []byte("null"), nil
		}
		return nil, r.err
	}
	return r.buf.Bytes(), nil
}

type jsonMySQLTextRenderer struct {
	buf             bytes.Buffer
	err             error
	ignoreDecodeErr bool
}

func (r *jsonMySQLTextRenderer) setErr(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *jsonMySQLTextRenderer) isDataShort(data []byte, expected int) bool {
	if r.err != nil {
		return true
	}
	if len(data) < expected {
		r.setErr(errors.Errorf("data len %d < expected %d", len(data), expected))
		return true
	}
	return false
}

func (r *jsonMySQLTextRenderer) renderValue(tp byte, data []byte) {
	if r.err != nil {
		return
	}
	switch tp {
	case JSONB_SMALL_OBJECT:
		r.renderObjectOrArray(data, true, true)
	case JSONB_LARGE_OBJECT:
		r.renderObjectOrArray(data, false, true)
	case JSONB_SMALL_ARRAY:
		r.renderObjectOrArray(data, true, false)
	case JSONB_LARGE_ARRAY:
		r.renderObjectOrArray(data, false, false)
	case JSONB_LITERAL:
		r.renderLiteral(data)
	case JSONB_INT16:
		if r.isDataShort(data, 2) {
			return
		}
		r.buf.WriteString(strconv.FormatInt(int64(mysql.ParseBinaryInt16(data[:2])), 10))
	case JSONB_UINT16:
		if r.isDataShort(data, 2) {
			return
		}
		r.buf.WriteString(strconv.FormatUint(uint64(mysql.ParseBinaryUint16(data[:2])), 10))
	case JSONB_INT32:
		if r.isDataShort(data, 4) {
			return
		}
		r.buf.WriteString(strconv.FormatInt(int64(mysql.ParseBinaryInt32(data[:4])), 10))
	case JSONB_UINT32:
		if r.isDataShort(data, 4) {
			return
		}
		r.buf.WriteString(strconv.FormatUint(uint64(mysql.ParseBinaryUint32(data[:4])), 10))
	case JSONB_INT64:
		if r.isDataShort(data, 8) {
			return
		}
		r.buf.WriteString(strconv.FormatInt(mysql.ParseBinaryInt64(data[:8]), 10))
	case JSONB_UINT64:
		if r.isDataShort(data, 8) {
			return
		}
		r.buf.WriteString(strconv.FormatUint(mysql.ParseBinaryUint64(data[:8]), 10))
	case JSONB_DOUBLE:
		if r.isDataShort(data, 8) {
			return
		}
		r.buf.WriteString(formatMySQLDouble(mysql.ParseBinaryFloat64(data[:8])))
	case JSONB_STRING:
		r.renderString(data)
	case JSONB_OPAQUE:
		r.renderOpaque(data)
	default:
		r.setErr(errors.Errorf("invalid json type %d", tp))
	}
}

func (r *jsonMySQLTextRenderer) renderLiteral(data []byte) {
	if r.isDataShort(data, 1) {
		return
	}
	switch data[0] {
	case JSONB_NULL_LITERAL:
		r.buf.WriteString("null")
	case JSONB_TRUE_LITERAL:
		r.buf.WriteString("true")
	case JSONB_FALSE_LITERAL:
		r.buf.WriteString("false")
	default:
		r.setErr(errors.Errorf("invalid literal %c", data[0]))
	}
}

func (r *jsonMySQLTextRenderer) renderObjectOrArray(data []byte, isSmall, isObject bool) {
	offsetSize := jsonbGetOffsetSize(isSmall)
	if r.isDataShort(data, 2*offsetSize) {
		return
	}
	count := decodeJSONCount(data, isSmall)
	size := decodeJSONCount(data[offsetSize:], isSmall)
	if r.isDataShort(data, size) {
		return
	}

	keyEntrySize := jsonbGetKeyEntrySize(isSmall)
	valueEntrySize := jsonbGetValueEntrySize(isSmall)
	headerSize := 2*offsetSize + count*valueEntrySize
	if isObject {
		headerSize += count * keyEntrySize
	}
	if headerSize > size {
		r.setErr(errors.Errorf("header size %d > size %d", headerSize, size))
		return
	}

	// Decode keys up front (only when iterating an object) so they can be
	// emitted in order alongside the values.
	var keys [][]byte
	if isObject {
		keys = make([][]byte, count)
		for i := 0; i < count; i++ {
			entryOffset := 2*offsetSize + keyEntrySize*i
			keyOffset := decodeJSONCount(data[entryOffset:], isSmall)
			keyLength := int(mysql.ParseBinaryUint16(data[entryOffset+offsetSize : entryOffset+offsetSize+2]))
			if keyOffset < headerSize {
				r.setErr(errors.Errorf("invalid key offset %d, must >= %d", keyOffset, headerSize))
				return
			}
			if r.isDataShort(data, keyOffset+keyLength) {
				return
			}
			keys[i] = data[keyOffset : keyOffset+keyLength]
		}
	}

	if isObject {
		r.buf.WriteByte('{')
	} else {
		r.buf.WriteByte('[')
	}
	for i := 0; i < count; i++ {
		if i > 0 {
			r.buf.WriteString(", ")
		}
		if isObject {
			r.buf.WriteByte('"')
			writeJSONString(&r.buf, keys[i])
			r.buf.WriteString(`": `)
		}

		entryOffset := 2*offsetSize + valueEntrySize*i
		if isObject {
			entryOffset += keyEntrySize * count
		}
		tp := data[entryOffset]
		if isInlineValue(tp, isSmall) {
			r.renderValue(tp, data[entryOffset+1:entryOffset+valueEntrySize])
			if r.err != nil {
				return
			}
			continue
		}
		valueOffset := decodeJSONCount(data[entryOffset+1:], isSmall)
		if r.isDataShort(data, valueOffset) {
			return
		}
		r.renderValue(tp, data[valueOffset:])
		if r.err != nil {
			return
		}
	}
	if isObject {
		r.buf.WriteByte('}')
	} else {
		r.buf.WriteByte(']')
	}
}

func (r *jsonMySQLTextRenderer) renderString(data []byte) {
	l, n, err := decodeJSONVarLen(data)
	if err != nil {
		r.setErr(err)
		return
	}
	if r.isDataShort(data, l+n) {
		return
	}
	r.buf.WriteByte('"')
	writeJSONString(&r.buf, data[n:n+l])
	r.buf.WriteByte('"')
}

func (r *jsonMySQLTextRenderer) renderOpaque(data []byte) {
	if r.isDataShort(data, 1) {
		return
	}
	opaqueTp := data[0]
	data = data[1:]
	l, n, err := decodeJSONVarLen(data)
	if err != nil {
		r.setErr(err)
		return
	}
	if r.isDataShort(data, l+n) {
		return
	}
	payload := data[n : n+l]
	switch opaqueTp {
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		if len(payload) < 2 {
			r.setErr(errors.Errorf("decimal payload too short: %d", len(payload)))
			return
		}
		precision := int(payload[0])
		scale := int(payload[1])
		v, _, derr := decodeDecimal(payload[2:], precision, scale, false /*useDecimal*/)
		if derr != nil {
			r.setErr(derr)
			return
		}
		// decodeDecimal returns the canonical text form when useDecimal=false.
		// MySQL formats JSON DECIMAL values as plain numbers (no quotes), so
		// write the value directly.
		s, ok := v.(string)
		if !ok {
			r.setErr(errors.Errorf("decimal decode produced %T, expected string", v))
			return
		}
		r.buf.WriteString(s)
	case mysql.MYSQL_TYPE_TIME:
		r.buf.WriteByte('"')
		r.buf.WriteString(formatJSONTime(payload))
		r.buf.WriteByte('"')
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		r.buf.WriteByte('"')
		r.buf.WriteString(formatJSONDateTime(opaqueTp, payload))
		r.buf.WriteByte('"')
	default:
		// MySQL serialises unknown opaque types as base64("opaque-binary").
		// "SELECT json_col" yields "base64:typeNN:..."; emit the hex form
		// surrounded by quotes so the consumer at least sees a stable string.
		r.buf.WriteByte('"')
		r.buf.WriteString("opaque:type=")
		r.buf.WriteString(strconv.Itoa(int(opaqueTp)))
		r.buf.WriteByte(':')
		r.buf.WriteString(hex.EncodeToString(payload))
		r.buf.WriteByte('"')
	}
}

// formatMySQLDouble formats a float64 the way MySQL does in JSON text:
// whole-number doubles keep a trailing ".0", and other values use the
// shortest round-trippable decimal/scientific form that strconv produces.
// This mirrors FloatWithTrailingZero.MarshalJSON.
func formatMySQLDouble(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// MySQL refuses to store NaN/Inf in JSON; we should never see one,
		// but emit a safe fallback rather than corrupt the surrounding doc.
		return "null"
	}
	if f == math.Trunc(f) && !math.IsInf(f, 0) {
		// Use FormatFloat 'f' with 1 digit after the decimal so 1.0 stays
		// "1.0". Matches FloatWithTrailingZero.MarshalJSON.
		return strconv.FormatFloat(f, 'f', 1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func formatJSONTime(payload []byte) string {
	if len(payload) < 8 {
		return "00:00:00"
	}
	v := mysql.ParseBinaryInt64(payload[:8])
	if v == 0 {
		return "00:00:00"
	}
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	intPart := v >> 24
	hour := (intPart >> 12) % (1 << 10)
	minute := (intPart >> 6) % (1 << 6)
	sec := intPart % (1 << 6)
	frac := v % (1 << 24)
	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, sec, frac)
}

func formatJSONDateTime(opaqueTp byte, payload []byte) string {
	if len(payload) < 8 {
		return "0000-00-00 00:00:00"
	}
	v := mysql.ParseBinaryInt64(payload[:8])
	if v == 0 {
		if opaqueTp == mysql.MYSQL_TYPE_DATE {
			return "0000-00-00"
		}
		return "0000-00-00 00:00:00"
	}
	if v < 0 {
		v = -v
	}
	intPart := v >> 24
	ymd := intPart >> 17
	ym := ymd >> 5
	hms := intPart % (1 << 17)
	year := ym / 13
	month := ym % 13
	day := ymd % (1 << 5)
	hour := hms >> 12
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	frac := v % (1 << 24)
	if opaqueTp == mysql.MYSQL_TYPE_DATE {
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)
}

func decodeJSONCount(data []byte, isSmall bool) int {
	if isSmall {
		return int(mysql.ParseBinaryUint16(data[:2]))
	}
	return int(mysql.ParseBinaryUint32(data[:4]))
}

// decodeJSONVarLen decodes the variable-length integer used by JSONB. It
// mirrors jsonBinaryDecoder.decodeVariableLength but is reusable without
// holding an instance.
func decodeJSONVarLen(data []byte) (length, consumed int, err error) {
	maxCount := len(data)
	if maxCount > 5 {
		maxCount = 5
	}
	var l uint64
	for pos := 0; pos < maxCount; pos++ {
		v := data[pos]
		l |= uint64(v&0x7F) << uint(7*pos)
		if v&0x80 == 0 {
			if l > math.MaxUint32 {
				return 0, 0, errors.Errorf("variable length %d exceeds %d", l, int64(math.MaxUint32))
			}
			return int(l), pos + 1, nil
		}
	}
	return 0, 0, errors.New("decode variable length failed")
}

// writeJSONString writes the contents of s as a JSON-escaped string
// (without surrounding quotes). The escape set matches what
// goccy/go-json produces for plain Go strings: control chars, quote,
// backslash, plus invalid UTF-8 bytes as U+FFFD. The U+FFFD substitution
// is lossy and diverges from MySQL, which preserves raw bytes — callers
// that need exact round-tripping must ensure inputs are valid UTF-8.
func writeJSONString(buf *bytes.Buffer, s []byte) {
	const hexdigits = "0123456789abcdef"
	i := 0
	for i < len(s) {
		c := s[i]
		if c < 0x20 || c == '"' || c == '\\' {
			switch c {
			case '"':
				buf.WriteString(`\"`)
			case '\\':
				buf.WriteString(`\\`)
			case '\b':
				buf.WriteString(`\b`)
			case '\f':
				buf.WriteString(`\f`)
			case '\n':
				buf.WriteString(`\n`)
			case '\r':
				buf.WriteString(`\r`)
			case '\t':
				buf.WriteString(`\t`)
			default:
				buf.WriteString(`\u00`)
				buf.WriteByte(hexdigits[c>>4])
				buf.WriteByte(hexdigits[c&0xF])
			}
			i++
			continue
		}
		if c < utf8.RuneSelf {
			buf.WriteByte(c)
			i++
			continue
		}
		// Multi-byte UTF-8: pass through if valid, replace if not.
		_, size := utf8.DecodeRune(s[i:])
		if size == 1 {
			buf.WriteString(`�`)
		} else {
			buf.Write(s[i : i+size])
		}
		i += size
	}
}
