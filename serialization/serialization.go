// Package serialization is for working with the mysql::serialization format
//
// mysql::serialization is a serialization format introduced with tagged GTIDs
//
// https://dev.mysql.com/doc/dev/mysql-server/latest/PageLibsMysqlSerialization.html
package serialization

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"strings"
)

// Message is a mysql::serialization message
type Message struct {
	Version uint8 // >= 0
	Format  Format
}

func (m *Message) String() (text string) {
	text += fmt.Sprintf("Message (version: %d)", m.Version)
	for _, line := range strings.Split(m.Format.String(), "\n") {
		text += "\n  " + line
	}
	return
}

// GetFieldByName returns a field if the name matches and an error if there is no match
func (m *Message) GetFieldByName(name string) (Field, error) {
	for _, f := range m.Format.Fields {
		if f.Name == name {
			return f, nil
		}
	}
	return Field{}, fmt.Errorf("field not found: %s", name)
}

// Format is describing a `message_format`
type Format struct {
	Size                  uint64
	LastNonIgnorableField int
	Fields                []Field
}

func (f *Format) String() (text string) {
	text += fmt.Sprintf("Format (Size: %d, LastNonIgnorableField: %d)\n",
		f.Size, f.LastNonIgnorableField)
	for _, f := range f.Fields {
		text += fmt.Sprintf("Field %02d (Name: %s, Skipped: %t, Type: %T)\n",
			f.ID, f.Name, f.Skipped, f.Type)
		if f.Type != nil {
			text += fmt.Sprintf("  Value: %s\n", f.Type.String())
		}
	}
	return text
}

// Field represents a `message_field`
type Field struct {
	ID       int
	Type     FieldType
	Optional bool
	Name     string
	Skipped  bool
}

// FieldType represents a `type_field`
type FieldType interface {
	fmt.Stringer
}

// FieldIntFixed is for values with a fixed length.
// This is also known as the 'fixlen_integer_format'.
// The encoded value can vary be between 1 and 2 times
// of that of the value before encoding.
type FieldIntFixed struct {
	Length int // Length of value before encoding, encoded value can be more
	Value  []byte
}

func (f FieldIntFixed) String() string {
	if f.Value == nil {
		return ""
	}
	return fmt.Sprintf("0x%x", f.Value)
}

// FieldIntVar is using the signed integer variant of the 'varlen_integer_format'
// and encodes a value as a byte sequence of 1-9 bytes depending on the value.
type FieldIntVar struct {
	Value int64
}

func (f FieldIntVar) String() string {
	return fmt.Sprintf("%d", f.Value)
}

// FieldUintVar is using the usigned integer variant of the 'varlen_integer_format'
// and encodes a value as a byte sequence of 1-9 bytes depending on the value.
type FieldUintVar struct {
	Value uint64
}

func (f FieldUintVar) String() string {
	return fmt.Sprintf("%d", f.Value)
}

// FieldString is a 'string_format' field
type FieldString struct {
	Value string
}

func (f FieldString) String() string {
	return f.Value
}

func Unmarshal(data []byte, v interface{}) error {
	switch m := v.(type) {
	case *Message:
		m.Version = data[0] >> 1
		err := Unmarshal(data[1:], &m.Format)
		if err != nil {
			return err
		}
	case *Format:
		pos := uint64(0)
		m.Size = uint64(data[pos] >> 1)
		pos++
		m.LastNonIgnorableField = int(data[pos] >> 1)
		pos++

		for i := 0; i < len(m.Fields); i++ {
			if int(pos)+1 > len(data) || int(data[pos]>>1) != i {
				// The field number we got doesn't match what we expect,
				// so a field was skipped.
				m.Fields[i].ID = i
				m.Fields[i].Skipped = true
				continue
			}
			m.Fields[i].ID = int(data[pos] >> 1)
			pos++
			var n uint64
			var err error
			switch f := m.Fields[i].Type.(type) {
			case FieldIntFixed:
				f.Value, n, err = decodeFixed(data, pos, f.Length)
				if err != nil {
					return err
				}
				m.Fields[i].Type = f
			case FieldUintVar:
				var val interface{}
				val, n, err = decodeVar(data, pos, true)
				if err != nil {
					return err
				}
				if uintval, ok := val.(uint64); ok {
					f.Value = uintval
				} else {
					return errors.New("unexpected type, expecting uint64")
				}
				m.Fields[i].Type = f
			case FieldIntVar:
				var val interface{}
				val, n, err = decodeVar(data, pos, false)
				if err != nil {
					return err
				}
				if intval, ok := val.(int64); ok {
					f.Value = intval
				} else {
					return errors.New("unexpected type, expecting int64")
				}
				m.Fields[i].Type = f
			case FieldString:
				f.Value, n, err = decodeString(data, pos)
				if err != nil {
					return err
				}
				m.Fields[i].Type = f
			default:
				return fmt.Errorf("unsupported field type: %T", m.Fields[i].Type)
			}
			pos = n
		}

	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
	return nil
}

func decodeString(data []byte, pos uint64) (string, uint64, error) {
	if len(data) < int(pos)+1 {
		return "", pos, errors.New("string truncated, expected at least one byte")
	}
	strLen := int(data[pos] >> 1)
	pos++
	if len(data) < int(pos)+strLen {
		return "", pos, fmt.Errorf("string truncated, expected length: %d", strLen)
	}
	return string(data[pos : pos+uint64(strLen)]), pos + uint64(strLen), nil
}

func decodeFixed(data []byte, pos uint64, intlen int) ([]byte, uint64, error) {
	var b bytes.Buffer

	for {
		if len(data) < int(pos)+1 {
			return b.Bytes(), pos, errors.New("data truncated")
		}
		if data[pos]%2 == 0 {
			b.WriteByte(data[pos] >> 1)
		} else {
			if len(data) < int(pos)+2 {
				return b.Bytes(), pos, errors.New("data truncated")
			}
			switch data[pos+1] {
			case 0x2:
				b.WriteByte((data[pos] >> 2) + 0x80)
			case 0x3:
				b.WriteByte((data[pos] >> 2) + 0xc0)
			default:
				return nil, pos, fmt.Errorf("unknown decoding for %v", data[pos])
			}
			pos++
		}
		pos++
		if b.Len() == intlen {
			break
		}
	}
	return b.Bytes(), pos, nil
}

func decodeVar(data []byte, pos uint64, unsigned bool) (interface{}, uint64, error) {
	if len(data) < int(pos)+1 {
		return 0, pos, errors.New("data truncated")
	}
	flen := trailingOneBitCount(data[pos]) + 1
	if len(data) < int(pos)+flen {
		return 0, pos, fmt.Errorf("truncated data, expected length: %d", flen)
	}
	var tNumBytes [8]byte
	copy(tNumBytes[:], data[pos:int(pos)+flen])
	tNum := binary.LittleEndian.Uint64(tNumBytes[:])
	pos += uint64(flen)
	if unsigned {
		return tNum >> flen, pos, nil
	}
	if positive := (tNum>>flen)&1 == 0; positive {
		return int64(tNum >> (flen + 1)), pos, nil
	}
	return int64(-(1 + (tNum >> (flen + 1)))), pos, nil
}

func trailingOneBitCount(b byte) int {
	return bits.TrailingZeros8(^b)
}
