package replication

import (
	"bytes"
	"math"
	"strconv"

	"github.com/goccy/go-json"
)

// This file holds the MySQL-text marshalers used by jsonBinaryDecoder
// when its mysqlTextMode flag is set. Wrapping the leaf decode returns
// in these types lets the existing json.Marshal pass produce JSON text
// that is faithful to each JSONB value's original type tag where the
// JSON text grammar can express it (DOUBLE 1.0 stays "1.0"; NEWDECIMAL
// stays unquoted; etc.) and preserves the JSONB key order.
//
// Caveats:
//   - Inter-token whitespace is compact (no space after ',' or ':'),
//     unlike MySQL's "SELECT json_col" form. DOUBLE scalars are
//     byte-identical to MySQL's own text (see jsonMySQLDouble).
//   - NEWDECIMAL is the one tag that cannot be preserved on text
//     round-trip: MySQL's JSON text grammar has no decimal literal, so
//     re-inserting the unquoted number yields a JSON DOUBLE, not the
//     original JSONB_OPAQUE NEWDECIMAL. The numeric value still
//     round-trips; only the opaque type tag is lost. All other tags
//     covered here do reproduce the original JSONB binary on re-insert.

// jsonString carries a JSONB string payload as raw bytes so MarshalJSON
// can pass non-ASCII bytes through verbatim. MySQL JSON is byte-
// transparent, so bytes >= 0x20 (other than '"' and '\\') are written
// without UTF-8 validation -- unlike the default encoding/json path which
// replaces invalid UTF-8 with U+FFFD.
type jsonString string

func (s jsonString) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, len(s)+2))
	buf.WriteByte('"')
	writeJSONString(buf, []byte(s))
	buf.WriteByte('"')
	return buf.Bytes(), nil
}

// jsonRawNumber emits its bytes unquoted. Used for JSONB OPAQUE
// NEWDECIMAL values: MySQL renders these as plain numbers in JSON text,
// not as quoted strings.
type jsonRawNumber string

func (n jsonRawNumber) MarshalJSON() ([]byte, error) {
	return []byte(n), nil
}

// jsonMySQLDouble formats a float64 the way MySQL renders a JSON DOUBLE
// in JSON text. Byte-identity matters because consumers re-insert this
// text, and a spelling MySQL would not have produced can re-read as a
// different value: expanding 6.02214076e23 to 24 fixed-point digits
// re-reads as 6.0221407600000005e23.
//
// This does not make the round-trip lossless. MySQL's JSON text parser
// misrounds some 16-17 significant-digit doubles by 1 ulp whatever the
// spelling (MySQL bugs #116160 and #112904), including values MySQL
// itself renders that way. Matching the server is the contract here;
// out-guessing its parser is not.
type jsonMySQLDouble float64

func (f jsonMySQLDouble) MarshalJSON() ([]byte, error) {
	return []byte(formatMySQLDouble(float64(f))), nil
}

// jsonObject preserves JSONB key order (length-then-bytes, which is what
// MySQL emits) instead of going through map[string]any, which json.Marshal
// would sort lexicographically.
type jsonObject struct {
	keys   []string
	values []any
}

func (o jsonObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		writeJSONString(&buf, []byte(k))
		buf.WriteString(`":`)
		vb, err := json.Marshal(o.values[i])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// formatMySQLDouble returns the bytes MySQL emits for a JSON DOUBLE.
// The rule below was derived empirically and checked byte-for-byte
// against MySQL 8.0, 8.4 and 9.x over a corpus spanning the format
// boundaries.
//
// The significant digits are the shortest round-trip string, which Go's
// strconv precision -1 already matches. Three things differ from Go's
// defaults:
//
//   - Fixed-vs-scientific selection. With decpt the decimal point
//     position (f = 0.digits * 10^decpt) and nd the digit count, MySQL
//     uses fixed-point iff decpt >= -14 && (decpt <= 15 || nd > decpt).
//     Since nd <= 17 for any float64, that is decpt in [-14, 15] plus
//     the boundary case decpt == 16 && nd == 17, e.g.
//     "1234567890123456.7".
//   - Exponents carry no '+' and no zero padding: MySQL writes "1.5e-5"
//     where Go writes "1.5e-05".
//   - Integral fixed-point values get a ".0" so they re-parse as JSON
//     DOUBLE rather than JSON INTEGER: 1e14 renders as
//     "100000000000000.0", while 1e16 stays "1e16".
func formatMySQLDouble(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// MySQL refuses to store NaN/Inf in JSON; emit a safe fallback
		// rather than corrupt the surrounding document.
		return "null"
	}
	// The 'e' form carries both inputs to the selection: the shortest
	// digit string and the decimal exponent.
	sci := strconv.AppendFloat(make([]byte, 0, 32), f, 'e', -1, 64)
	ePos := bytes.IndexByte(sci, 'e')
	exp, err := strconv.Atoi(string(sci[ePos+1:]))
	if err != nil {
		// Unreachable: 'e'-formatted output always ends in "e±NN".
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	mant := sci[:ePos] // "d" or "d.ddd", optionally '-'-prefixed
	neg := mant[0] == '-'
	if neg {
		mant = mant[1:]
	}
	nd := len(mant)
	if nd > 1 {
		nd-- // drop the '.' at mant[1]
	}
	decpt := exp + 1 // f = 0.digits * 10^decpt

	if decpt >= -14 && (decpt <= 15 || nd > decpt) {
		// Longest fixed-point form is 34 bytes: sign, "0.", 14 zeros, 17 digits.
		out := strconv.AppendFloat(make([]byte, 0, 40), f, 'f', -1, 64)
		if bytes.IndexByte(out, '.') < 0 {
			out = append(out, '.', '0')
		}
		return string(out)
	}

	// Scientific notation: reuse Go's mantissa, respell the exponent.
	out := make([]byte, 0, len(sci))
	if neg {
		out = append(out, '-')
	}
	out = append(out, mant...)
	out = append(out, 'e')
	out = strconv.AppendInt(out, int64(exp), 10)
	return string(out)
}

// writeJSONString writes s as the contents of a JSON string (no
// surrounding quotes), with byte-transparent semantics: bytes >= 0x20
// other than '"' and '\\' are written verbatim, including high-bit bytes
// that may not form valid UTF-8.
func writeJSONString(buf *bytes.Buffer, s []byte) {
	const hexdigits = "0123456789abcdef"
	for i := range len(s) {
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
			continue
		}
		buf.WriteByte(c)
	}
}
