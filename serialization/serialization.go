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
	"io"
	"math/bits"
	"slices"
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

type Field struct {
	ID       int
	Type     FieldType
	Optional bool
	Name     string
	Skipped  bool
}

type FieldType interface {
	fmt.Stringer
}

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

type FieldIntVar struct {
	Value int64
}

func (f FieldIntVar) String() string {
	return fmt.Sprintf("%d", f.Value)
}

type FieldUintVar struct {
	Value uint64
}

func (f FieldUintVar) String() string {
	return fmt.Sprintf("%d", f.Value)
}

type FieldString struct {
	Value string
}

func (f FieldString) String() string {
	return f.Value
}

func Unmarshal(data []byte, v interface{}) error {
	r := bytes.NewReader(data)
	switch m := v.(type) {
	case *Message:
		messageLen := 1
		tmpVer := make([]byte, messageLen)
		_, err := r.Read(tmpVer)
		if err != nil {
			return err
		}
		m.Version = tmpVer[0] / 2

		err = Unmarshal(data[messageLen:], &m.Format)
		if err != nil {
			return err
		}
	case *Format:
		formatLen := 2
		tmpFormat := make([]byte, formatLen)
		_, err := r.Read(tmpFormat)
		if err != nil {
			return err
		}
		m.Size = uint64(tmpFormat[0] / 2)
		m.LastNonIgnorableField = int(tmpFormat[1] / 2)

		for i := 0; i < len(m.Fields); i++ {
			tmpField := make([]byte, 1)
			_, err := r.Read(tmpField)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			if int(tmpField[0]/2) != i {
				// The field number we got doesn't match what we expect,
				// so a field was skipped. Rewind the reader and skip.
				m.Fields[i].ID = i
				m.Fields[i].Skipped = true
				_, err := r.Seek(-1, io.SeekCurrent)
				if err != nil {
					return err
				}
				continue
			}
			m.Fields[i].ID = int(tmpField[0] / 2)
			switch f := m.Fields[i].Type.(type) {
			case FieldIntFixed:
				f.Value, err = decodeFixed(r, f.Length)
				if err != nil {
					return err
				}
				m.Fields[i].Type = f
			case FieldUintVar:
				val, err := decodeVar(r, true)
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
				val, err := decodeVar(r, false)
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
				f.Value, err = decodeString(r)
				if err != nil {
					return err
				}
				m.Fields[i].Type = f
			default:
				return fmt.Errorf("unsupported field type: %T", m.Fields[i].Type)
			}
		}

	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
	return nil
}

func decodeString(r io.Reader) (string, error) {
	firstByte := make([]byte, 1)
	_, err := r.Read(firstByte)
	if err != nil {
		return "", err
	}
	strBytes := make([]byte, firstByte[0]/2)
	n, err := r.Read(strBytes)
	if err != nil {
		return "", err
	}
	if n != int(firstByte[0]/2) {
		return "", fmt.Errorf("only read %d bytes, expected %d", n, firstByte[0]/2)
	}
	return string(strBytes), nil
}

func decodeFixed(r io.Reader, len int) ([]byte, error) {
	var b bytes.Buffer

	tmpInt := make([]byte, 1)
	for {
		_, err := r.Read(tmpInt)
		if err != nil {
			return nil, err
		}
		if tmpInt[0]%2 == 0 {
			b.WriteByte(tmpInt[0] / 2)
		} else {
			tmpInt2 := make([]byte, 1)
			_, err := r.Read(tmpInt2)
			if err != nil {
				return nil, err
			}
			switch tmpInt2[0] {
			case 0x2:
				b.WriteByte((tmpInt[0] >> 2) + 0x80)
			case 0x3:
				b.WriteByte((tmpInt[0] >> 2) + 0xc0)
			default:
				return nil, fmt.Errorf("unknown decoding for %v", tmpInt2[0])
			}
		}
		if b.Len() == len {
			break
		}
	}
	return b.Bytes(), nil
}

func decodeVar(r io.ReadSeeker, unsigned bool) (interface{}, error) {
	firstByte := make([]byte, 1)
	_, err := r.Read(firstByte)
	if err != nil {
		return 0, err
	}
	tb := trailingOneBitCount(firstByte[0])
	_, err = r.Seek(-1, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	fieldBytes := make([]byte, tb+1)
	n, err := r.Read(fieldBytes)
	if err != nil {
		return 0, err
	}
	if n != tb+1 {
		return 0, fmt.Errorf("only read %d bytes, expected %d", n, tb+1)
	}
	var tNum uint64
	switch len(fieldBytes) {
	case 1:
		tNum = uint64(fieldBytes[0])
	case 2:
		tNum = uint64(binary.LittleEndian.Uint16(fieldBytes))
	case 3:
		tNum = uint64(binary.LittleEndian.Uint32(
			slices.Concat(fieldBytes, []byte{0x0})))
	case 4:
		tNum = uint64(binary.LittleEndian.Uint32(fieldBytes))
	case 5:
		tNum = binary.LittleEndian.Uint64(
			slices.Concat(fieldBytes, []byte{0x0, 0x0, 0x0}))
	case 6:
		tNum = binary.LittleEndian.Uint64(
			slices.Concat(fieldBytes, []byte{0x0, 0x0}))
	case 7:
		tNum = binary.LittleEndian.Uint64(
			slices.Concat(fieldBytes, []byte{0x0}))
	case 8:
		tNum = binary.LittleEndian.Uint64(fieldBytes)
	}
	if unsigned {
		return tNum >> (tb + 1), nil
	}
	return int64(tNum >> (tb + 2)), nil
}

func trailingOneBitCount(b byte) int {
	return bits.TrailingZeros8(^b)
}
