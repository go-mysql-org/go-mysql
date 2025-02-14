package serialization

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

// mysql::serialization is a serialization format introduced with tagged GTIDs
//
// https://dev.mysql.com/doc/dev/mysql-server/latest/PageLibsMysqlSerialization.html

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

func (m *Message) GetFieldByName(name string) (Field, error) {
	for _, f := range m.Format.Fields {
		if f.Name == name {
			return f, nil
		}
	}
	return Field{}, fmt.Errorf("field not found: %s", name)
}

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
	Value    uint64
	Unsigned bool
}

func (f FieldIntVar) String() string {
	return fmt.Sprintf("%d", f.Value)
}

type FieldString struct {
	Value string
}

func (f FieldString) String() string {
	return f.Value
}

type Marshaler interface {
	MarshalMySQLSerial() ([]byte, error)
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
				tmpVal, err := decodeFixed(r, f.Length)
				if err != nil {
					return err
				}
				f.Value = tmpVal
				m.Fields[i].Type = f
			case FieldIntVar:
				firstByte := make([]byte, 1)
				_, err := r.Read(firstByte)
				if err != nil {
					return err
				}
				tb := trailingOneBitCount(firstByte[0])
				_, err = r.Seek(-1, io.SeekCurrent)
				if err != nil {
					return err
				}
				fieldBytes := make([]byte, tb+1)
				_, err = r.Read(fieldBytes)
				if err != nil {
					return err
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
				if f.Unsigned {
					f.Value = tNum >> (tb + 2) * 2
				} else {
					f.Value = tNum >> (tb + 2)
				}
				m.Fields[i].Type = f
			case FieldString:
				firstByte := make([]byte, 1)
				_, err := r.Read(firstByte)
				if err != nil {
					return err
				}
				strBytes := make([]byte, firstByte[0]/2)
				_, err = r.Read(strBytes)
				if err != nil {
					return err
				}
				f.Value = string(strBytes)
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

func trailingOneBitCount(b byte) (count int) {
	var i byte = 0x1
	for {
		if b&i == 0 {
			break
		}
		count++
		if i >= 0x80 {
			break
		}
		i = i << 1
	}
	return
}
