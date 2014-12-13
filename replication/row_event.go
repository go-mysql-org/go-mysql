package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	. "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/hack"
	"io"
	"strconv"
	"time"
)

type TableMapEvent struct {
	tableIDSize int

	TableID uint64

	Flags uint16

	Schema []byte
	Table  []byte

	ColumnCount uint64
	ColumnType  []byte
	ColumnMeta  []uint16

	//len = (ColumnCount + 7) / 8
	NullBitmap []byte
}

func (e *TableMapEvent) Decode(data []byte) error {
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.tableIDSize])
	pos += e.tableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	schemaLength := data[pos]
	pos++

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	tableLength := data[pos]
	pos++

	e.Table = data[pos : pos+int(tableLength)]
	pos += int(tableLength)

	//skip 0x00
	pos++

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	e.ColumnType = data[pos : pos+int(e.ColumnCount)]
	pos += int(e.ColumnCount)

	var err error
	var metaData []byte
	if metaData, _, n, err = LengthEnodedString(data[pos:]); err != nil {
		return err
	}

	if err = e.decodeMeta(metaData); err != nil {
		return err
	}

	pos += n

	if len(data[pos:]) != nullBitmapSize(int(e.ColumnCount)) {
		return io.EOF
	}

	e.NullBitmap = data[pos:]

	return nil
}

func isNullSet(nullBitmap []byte, i int) bool {
	return nullBitmap[i/8]&(1<<(uint(i)%8)) > 0
}

func nullBitmapSize(columnCount int) int {
	return int(columnCount+7) / 8
}

// see mysql sql/log_event.h
/*
	0 byte
	MYSQL_TYPE_DECIMAL
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR

	1 byte
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_GEOMETRY

	//maybe
	MYSQL_TYPE_TIME2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIMESTAMP2

	2 byte
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_NEWDECIMAL
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING

	This enumeration value is only used internally and cannot exist in a binlog.
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_ENUM
	MYSQL_TYPE_SET
	MYSQL_TYPE_TINY_BLOB
	MYSQL_TYPE_MEDIUM_BLOB
	MYSQL_TYPE_LONG_BLOB
*/
func (e *TableMapEvent) decodeMeta(data []byte) error {
	pos := 0
	e.ColumnMeta = make([]uint16, e.ColumnCount)
	for i, t := range e.ColumnType {
		switch t {
		case MYSQL_TYPE_STRING:
			var x uint16 = uint16(data[pos]) << 8 //real type
			x += uint16(data[pos+1])              //pack or field length
			e.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_NEWDECIMAL:
			var x uint16 = uint16(data[pos]) << 8 //precision
			x += uint16(data[pos+1])              //decimals
			e.ColumnMeta[i] = x
			pos += 2
		case MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT:
			e.ColumnMeta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case MYSQL_TYPE_BLOB,
			MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_GEOMETRY:
			e.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_TIME2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP2:
			e.ColumnMeta[i] = uint16(data[pos])
			pos++
		case MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB:
			return fmt.Errorf("unsupport type in binlog %d", t)
		default:
			e.ColumnMeta[i] = 0
		}
	}

	return nil
}

func (e *TableMapEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)
	fmt.Fprintf(w, "Flags: %d\n", e.Flags)
	fmt.Fprintf(w, "Schema: %s\n", e.Schema)
	fmt.Fprintf(w, "Table: %s\n", e.Table)
	fmt.Fprintf(w, "Column count: %d\n", e.ColumnCount)
	fmt.Fprintf(w, "Column type: \n%s", hex.Dump(e.ColumnType))
	fmt.Fprintf(w, "NULL bitmap: \n%s", hex.Dump(e.NullBitmap))
	fmt.Fprintln(w)
}

type RowsEvent struct {
	//0, 1, 2
	Version int

	tableIDSize int
	tables      map[uint64]*TableMapEvent
	needBitmap2 bool

	TableID uint64

	Flags uint16

	//if version == 2
	ExtraData []byte

	//lenenc_int
	ColumnCount uint64
	//len = (ColumnCount + 7) / 8
	ColumnBitmap1 []byte

	//if UPDATE_ROWS_EVENTv1 or v2
	//len = (ColumnCount + 7) / 8
	ColumnBitmap2 []byte

	//rows: invalid: int64, float64, bool, []byte, string
	Rows [][]interface{}
}

func (e *RowsEvent) Decode(data []byte) error {
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.tableIDSize])
	pos += e.tableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if e.Version == 2 {
		dataLen := binary.LittleEndian.Uint16(data[pos:])
		pos += 2

		e.ExtraData = data[pos : pos+int(dataLen-2)]
		pos += int(dataLen - 2)
	}

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	bitCount := nullBitmapSize(int(e.ColumnCount))
	e.ColumnBitmap1 = data[pos : pos+bitCount]
	pos += bitCount

	if e.needBitmap2 {
		e.ColumnBitmap2 = data[pos : pos+bitCount]
		pos += bitCount
	}

	tableEvent, ok := e.tables[e.TableID]
	if !ok {
		return fmt.Errorf("invalid table id %d, no correspond table map event", e.TableID)
	}

	var err error
	for len(data[pos:]) > 0 {
		if n, err = e.decodeRows(data[pos:], tableEvent); err != nil {
			return err
		}
		pos += n
	}

	return nil
}

func (e *RowsEvent) decodeRows(data []byte, table *TableMapEvent) (int, error) {
	rows := make([]interface{}, e.ColumnCount)

	pos := 0

	bitCount := nullBitmapSize(int(e.ColumnCount))
	nullBitmap := data[pos : pos+bitCount]
	pos += bitCount

	var n int
	var err error
	for i := 0; i < int(e.ColumnCount); i++ {
		if isNullSet(nullBitmap, i) {
			rows[i] = nil
			continue
		}

		rows[i], n, err = e.decodeValue(data[pos:], table.ColumnType[i], table.ColumnMeta[i])
		if err != nil {
			return 0, nil
		}
		pos += n
	}
	return pos, nil
}

// see mysql sql/log_event.cc log_event_print_value
func (e *RowsEvent) decodeValue(data []byte, tp byte, meta uint16) (v interface{}, n int, err error) {
	var length uint16 = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = uint16(b1) | (uint16((b0&0x30)^0x30) << 4)
				tp = byte(b0 | 0x30)
			} else {
				length = meta & 0xFF
			}
		} else {
			length = meta
		}
	}

	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_LONG:
		n = 4
		v = int64(binary.LittleEndian.Uint32(data))
	case MYSQL_TYPE_TINY:
		n = 1
		v = int64(data[0])
	case MYSQL_TYPE_SHORT:
		n = 2
		v = int64(binary.LittleEndian.Uint16(data))
	case MYSQL_TYPE_INT24:
		n = 3
		v = int64(FixedLengthInt(data[0:3]))
	case MYSQL_TYPE_LONGLONG:
		//em, maybe overflow for int64......
		n = 8
		v = int64(binary.LittleEndian.Uint64(data))
	case MYSQL_TYPE_NEWDECIMAL:
		prec := uint8(meta >> 8)
		scale := uint8(meta & 0xFF)
		var f string
		//return string first
		f, n, err = decodeDecimal(data, int(prec), int(scale))
		v = f
	case MYSQL_TYPE_FLOAT:
		n = 4
		v = int64(binary.LittleEndian.Uint32(data))
	case MYSQL_TYPE_DOUBLE:
		n = 8
		v = int64(binary.LittleEndian.Uint64(data))
	case MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		n = int(nbits+7) / 8
		//we don't handle bit here, only use its raw buffer
		v = data[0:n]
	case MYSQL_TYPE_TIMESTAMP:
		n = 4
		t := binary.LittleEndian.Uint32(data)
		v = time.Unix(int64(t), 0)
	case MYSQL_TYPE_TIMESTAMP2:
		// {
		//   char buf[MAX_DATE_STRING_REP_LENGTH];
		//   struct timeval tm;
		//   my_timestamp_from_binary(&tm, ptr, meta);
		//   int buflen= my_timeval_to_str(&tm, buf, meta);
		//   my_b_write(file, buf, buflen);
		//   my_snprintf(typestr, typestr_length, "TIMESTAMP(%d)", meta);
		//   return my_timestamp_binary_length(meta);
		// }

	case MYSQL_TYPE_DATETIME:
		n = 8
		i64 := binary.LittleEndian.Uint64(data)
		d := i64 / 1000000
		t := i64 % 1000000
		v = time.Date(int(d/10000),
			time.Month((d%10000)/100),
			int(d%100),
			int(t/10000),
			int((t%10000)/100),
			int(t%100),
			0,
			time.UTC).Format(TimeFormat)
	case MYSQL_TYPE_DATETIME2:
		// {
		//   char buf[MAX_DATE_STRING_REP_LENGTH];
		//   MYSQL_TIME ltime;
		//   longlong packed= my_datetime_packed_from_binary(ptr, meta);
		//   TIME_from_longlong_datetime_packed(&ltime, packed);
		//   int buflen= my_datetime_to_str(&ltime, buf, meta);
		//   my_b_write_quoted(file, (uchar *) buf, buflen);
		//   my_snprintf(typestr, typestr_length, "DATETIME(%d)", meta);
		//   return my_datetime_binary_length(meta);
		// }

	case MYSQL_TYPE_TIME:
		n = 3
		i32 := uint32(FixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "00:00:00"
		} else {
			sign := ""
			if i32 < 0 {
				sign = "-"
			}
			v = fmt.Sprintf("%s%02d:%02d:%02d", sign, i32/10000, (i32%10000)/100, i32%100)
		}
	case MYSQL_TYPE_TIME2:
		// {
		//   char buf[MAX_DATE_STRING_REP_LENGTH];
		//   MYSQL_TIME ltime;
		//   longlong packed= my_time_packed_from_binary(ptr, meta);
		//   TIME_from_longlong_time_packed(&ltime, packed);
		//   int buflen= my_time_to_str(&ltime, buf, meta);
		//   my_b_write_quoted(file, (uchar *) buf, buflen);
		//   my_snprintf(typestr, typestr_length, "TIME(%d)", meta);
		//   return my_time_binary_length(meta);
		// }

	case MYSQL_TYPE_NEWDATE:
		// {
		//   uint32 tmp= uint3korr(ptr);
		//   int part;
		//   char buf[11];
		//   char *pos= &buf[10];  // start from '\0' to the beginning

		//   /* Copied from field.cc */
		//   *pos--=0;					// End NULL
		//   part=(int) (tmp & 31);
		//   *pos--= (char) ('0'+part%10);
		//   *pos--= (char) ('0'+part/10);
		//   *pos--= ':';
		//   part=(int) (tmp >> 5 & 15);
		//   *pos--= (char) ('0'+part%10);
		//   *pos--= (char) ('0'+part/10);
		//   *pos--= ':';
		//   part=(int) (tmp >> 9);
		//   *pos--= (char) ('0'+part%10); part/=10;
		//   *pos--= (char) ('0'+part%10); part/=10;
		//   *pos--= (char) ('0'+part%10); part/=10;
		//   *pos=   (char) ('0'+part);
		//   my_b_printf(file , "'%s'", buf);
		//   my_snprintf(typestr, typestr_length, "DATE");
		//   return 3;
		// }

	case MYSQL_TYPE_YEAR:
		n = 1
		v = time.Date(int(data[0])+1900,
			time.January, 0, 0, 0, 0, 0,
			time.UTC).Format(TimeFormat)
	case MYSQL_TYPE_ENUM:
		// switch (meta & 0xFF) {
		// case 1:
		//   my_b_printf(file, "%d", (int) *ptr);
		//   my_snprintf(typestr, typestr_length, "ENUM(1 byte)");
		//   return 1;
		// case 2:
		//   {
		//     int32 i32= uint2korr(ptr);
		//     my_b_printf(file, "%d", i32);
		//     my_snprintf(typestr, typestr_length, "ENUM(2 bytes)");
		//     return 2;
		//   }
		// default:
		//   my_b_printf(file, "!! Unknown ENUM packlen=%d", meta & 0xFF);
		//   return 0;
		// }
		// break;

	case MYSQL_TYPE_SET:
		// my_b_write_bit(file, ptr , (meta & 0xFF) * 8);
		// my_snprintf(typestr, typestr_length, "SET(%d bytes)", meta & 0xFF);
		// return meta & 0xFF;

	case MYSQL_TYPE_BLOB:
		switch meta {
		case 1:

		case 2:
		case 3:
		case 4:
		default:
			err = fmt.Errorf("invalid blob packlen = %d", meta)
		}
		// switch (meta) {
		// case 1:
		//   length= *ptr;
		//   my_b_write_quoted(file, ptr + 1, length);
		//   my_snprintf(typestr, typestr_length, "TINYBLOB/TINYTEXT");
		//   return length + 1;
		// case 2:
		//   length= uint2korr(ptr);
		//   my_b_write_quoted(file, ptr + 2, length);
		//   my_snprintf(typestr, typestr_length, "BLOB/TEXT");
		//   return length + 2;
		// case 3:
		//   length= uint3korr(ptr);
		//   my_b_write_quoted(file, ptr + 3, length);
		//   my_snprintf(typestr, typestr_length, "MEDIUMBLOB/MEDIUMTEXT");
		//   return length + 3;
		// case 4:
		//   length= uint4korr(ptr);
		//   my_b_write_quoted(file, ptr + 4, length);
		//   my_snprintf(typestr, typestr_length, "LONGBLOB/LONGTEXT");
		//   return length + 4;
		// default:
		//   my_b_printf(file, "!! Unknown BLOB packlen=%d", length);
		//   return 0;
		// }

	case MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING:
		length = meta
		v, n = decodeString(data, length)
	case MYSQL_TYPE_STRING:
		v, n = decodeString(data, length)
	default:
		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
	}
	return
}

func decodeString(data []byte, length uint16) (v []byte, n int) {
	if length < 256 {
		length = uint16(data[0])

		n = int(length) + 1
		v = data[1:n]
	} else {
		length = binary.LittleEndian.Uint16(data[0:])
		n = int(length) + 2
		v = data[2:n]
	}

	return
}

const digitsPerInteger int = 9

var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func decodeDecimal(data []byte, precision int, decimals int) (string, int, error) {
	//see python mysql replication and https://github.com/jeremycole/mysql_binlog
	pos := 0

	integral := (precision - decimals)
	uncompIntegral := int(integral / digitsPerInteger)
	uncompFractional := int(decimals / digitsPerInteger)
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	//must copy the data for later change
	data = buf

	// Support negative
	// The sign is encoded in the high bit of the the byte
	// But this bit can also be used in the value
	value := int64(data[pos])
	var res bytes.Buffer
	var mask int64 = 0
	if value&0x80 == 0 {
		mask = -1
		res.WriteString("-")
	}

	//clear sign
	data[0] ^= 0x80

	size := compressedBytes[compIntegral]
	if size > 0 {
		value = int64(FixedLengthInt(data[pos:pos+size])) ^ mask
		res.WriteString(strconv.FormatInt(value, 10))
		pos += size
	}

	for i := 0; i < uncompIntegral; i++ {
		value = int64(binary.BigEndian.Uint32(data[pos:])) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	res.WriteString(".")

	for i := 0; i < uncompFractional; i++ {
		value = int64(binary.BigEndian.Uint32(data[pos:])) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	size = compressedBytes[compFractional]
	if size > 0 {
		value = int64(FixedLengthInt(data[pos:pos+size])) ^ mask
		pos += size

		res.WriteString(fmt.Sprintf("%0*d", compFractional, value))
	}

	return hack.String(res.Bytes()), pos, nil
}

func (e *RowsEvent) Dump(w io.Writer) {

}

type RowsQueryEvent struct {
	Query []byte
}

func (e *RowsQueryEvent) Decode(data []byte) error {
	//ignore length byte 1
	e.Query = data[1:]
	return nil
}

func (e *RowsQueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}
