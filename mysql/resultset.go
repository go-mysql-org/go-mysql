package mysql

import (
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/utils"
	"github.com/siddontang/go/hack"
)

type RowData []byte

func (p RowData) Parse(f []*Field, binary bool, dst []FieldValue) ([]FieldValue, error) {
	if binary {
		return p.ParseBinary(f, dst)
	} else {
		return p.ParseText(f, dst)
	}
}

func (p RowData) ParseText(f []*Field, dst []FieldValue) ([]FieldValue, error) {
	for len(dst) < len(f) {
		dst = append(dst, FieldValue{})
	}
	data := dst[:len(f)]

	var err error
	var v []byte
	var isNull bool
	var pos int = 0
	var n int = 0

	for i := range f {
		v, isNull, n, err = LengthEncodedString(p[pos:])
		if err != nil {
			return nil, errors.Trace(err)
		}

		pos += n

		if isNull {
			data[i].Type = FieldValueTypeNull
		} else {
			isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0

			switch f[i].Type {
			case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG, MYSQL_TYPE_LONG, MYSQL_TYPE_YEAR:
				if isUnsigned {
					data[i].Type = FieldValueTypeUnsigned
					data[i].Uint64, err = strconv.ParseUint(utils.ByteSliceToString(v), 10, 64)
				} else {
					data[i].Type = FieldValueTypeSigned
					data[i].Int64, err = strconv.ParseInt(utils.ByteSliceToString(v), 10, 64)
				}
			case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
				data[i].Type = FieldValueTypeFloat
				data[i].Float, err = strconv.ParseFloat(utils.ByteSliceToString(v), 64)
			default:
				data[i].Type = FieldValueTypeString
				data[i].String = append(data[i].String[:0], v...)
			}

			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	return data, nil
}

// ParseBinary parses the binary format of data
// see https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
func (p RowData) ParseBinary(f []*Field, dst []FieldValue) ([]FieldValue, error) {
	for len(dst) < len(f) {
		dst = append(dst, FieldValue{})
	}
	data := dst[:len(f)]

	if p[0] != OK_HEADER {
		return nil, ErrMalformPacket
	}

	pos := 1 + ((len(f) + 7 + 2) >> 3)

	nullBitmap := p[1:pos]

	var isNull bool
	var n int
	var err error
	var v []byte
	for i := range data {
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			data[i].Type = FieldValueTypeNull
			continue
		}

		isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0

		switch f[i].Type {
		case MYSQL_TYPE_NULL:
			data[i].Type = FieldValueTypeNull
			continue

		case MYSQL_TYPE_TINY:
			if isUnsigned {
				v := ParseBinaryUint8(p[pos : pos+1])
				data[i].Type = FieldValueTypeUnsigned
				data[i].Uint64 = uint64(v)
			} else {
				v := ParseBinaryInt8(p[pos : pos+1])
				data[i].Type = FieldValueTypeSigned
				data[i].Int64 = int64(v)
			}
			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if isUnsigned {
				v := ParseBinaryUint16(p[pos : pos+2])
				data[i].Type = FieldValueTypeUnsigned
				data[i].Uint64 = uint64(v)
			} else {
				v := ParseBinaryInt16(p[pos : pos+2])
				data[i].Type = FieldValueTypeSigned
				data[i].Int64 = int64(v)
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
			if isUnsigned {
				v := ParseBinaryUint32(p[pos : pos+4])
				data[i].Type = FieldValueTypeUnsigned
				data[i].Uint64 = uint64(v)
			} else {
				v := ParseBinaryInt32(p[pos : pos+4])
				data[i].Type = FieldValueTypeSigned
				data[i].Int64 = int64(v)
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
			if isUnsigned {
				data[i].Type = FieldValueTypeUnsigned
				data[i].Uint64 = ParseBinaryUint64(p[pos : pos+8])
			} else {
				data[i].Type = FieldValueTypeSigned
				data[i].Int64 = ParseBinaryInt64(p[pos : pos+8])
			}
			pos += 8
			continue

		case MYSQL_TYPE_FLOAT:
			v := ParseBinaryFloat32(p[pos : pos+4])
			data[i].Type = FieldValueTypeFloat
			data[i].Float = float64(v)
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			data[i].Type = FieldValueTypeFloat
			data[i].Float = ParseBinaryFloat64(p[pos : pos+8])
			pos += 8
			continue

		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY:
			v, isNull, n, err = LengthEncodedString(p[pos:])
			pos += n
			if err != nil {
				return nil, errors.Trace(err)
			}

			if !isNull {
				data[i].Type = FieldValueTypeString
				data[i].String = append(data[i].String[:0], v...)
				continue
			} else {
				data[i].Type = FieldValueTypeNull
				continue
			}

		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].String, err = FormatBinaryDate(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, errors.Trace(err)
			}

		case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].String, err = FormatBinaryDateTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, errors.Trace(err)
			}

		case MYSQL_TYPE_TIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].String, err = FormatBinaryTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, errors.Trace(err)
			}

		default:
			return nil, errors.Errorf("Stmt Unknown FieldType %d %s", f[i].Type, f[i].Name)
		}
	}

	return data, nil
}

type Resultset struct {
	Fields     []*Field
	FieldNames map[string]int
	Values     [][]FieldValue

	RawPkg []byte

	RowDatas []RowData
}

func (r *Resultset) reset(count int) {
	r.RawPkg = r.RawPkg[:0]

	r.Fields = r.Fields[:0]
	r.Values = r.Values[:0]
	r.RowDatas = r.RowDatas[:0]

	if r.FieldNames != nil {
		for k := range r.FieldNames {
			delete(r.FieldNames, k)
		}
	} else {
		r.FieldNames = make(map[string]int)
	}

	if count == 0 {
		return
	}

	if cap(r.Fields) < count {
		r.Fields = make([]*Field, count)
	} else {
		r.Fields = r.Fields[:count]
	}
}

func (r *Resultset) RowNumber() int {
	return len(r.Values)
}

func (r *Resultset) ColumnNumber() int {
	return len(r.Fields)
}

func (r *Resultset) GetValue(row, column int) (interface{}, error) {
	if row >= len(r.Values) || row < 0 {
		return nil, errors.Errorf("invalid row index %d", row)
	}

	if column >= len(r.Fields) || column < 0 {
		return nil, errors.Errorf("invalid column index %d", column)
	}

	val := &r.Values[row][column]

	switch val.Type {
	case FieldValueTypeUnsigned:
		return val.Uint64, nil
	case FieldValueTypeSigned:
		return val.Int64, nil
	case FieldValueTypeFloat:
		return val.Float, nil
	case FieldValueTypeString:
		return val.String, nil
	default: // FieldValueTypeNull
		return nil, nil
	}
}

func (r *Resultset) NameIndex(name string) (int, error) {
	if column, ok := r.FieldNames[name]; ok {
		return column, nil
	} else {
		return 0, errors.Errorf("invalid field name %s", name)
	}
}

func (r *Resultset) GetValueByName(row int, name string) (interface{}, error) {
	if column, err := r.NameIndex(name); err != nil {
		return nil, errors.Trace(err)
	} else {
		return r.GetValue(row, column)
	}
}

func (r *Resultset) IsNull(row, column int) (bool, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return false, err
	}

	return d == nil, nil
}

func (r *Resultset) IsNullByName(row int, name string) (bool, error) {
	if column, err := r.NameIndex(name); err != nil {
		return false, err
	} else {
		return r.IsNull(row, column)
	}
}

func (r *Resultset) GetUint(row, column int) (uint64, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return 0, err
	}

	switch v := d.(type) {
	case int:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetUintByName(row int, name string) (uint64, error) {
	if column, err := r.NameIndex(name); err != nil {
		return 0, err
	} else {
		return r.GetUint(row, column)
	}
}

func (r *Resultset) GetInt(row, column int) (int64, error) {
	v, err := r.GetUint(row, column)
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func (r *Resultset) GetIntByName(row int, name string) (int64, error) {
	v, err := r.GetUintByName(row, name)
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func (r *Resultset) GetFloat(row, column int) (float64, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return 0, err
	}

	switch v := d.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case nil:
		return 0, nil
	default:
		return 0, errors.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetFloatByName(row int, name string) (float64, error) {
	if column, err := r.NameIndex(name); err != nil {
		return 0, err
	} else {
		return r.GetFloat(row, column)
	}
}

func (r *Resultset) GetString(row, column int) (string, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return "", err
	}

	switch v := d.(type) {
	case string:
		return v, nil
	case []byte:
		return hack.String(v), nil
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", nil
	default:
		return "", errors.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetStringByName(row int, name string) (string, error) {
	if column, err := r.NameIndex(name); err != nil {
		return "", err
	} else {
		return r.GetString(row, column)
	}
}
