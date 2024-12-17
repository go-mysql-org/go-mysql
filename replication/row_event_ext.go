package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/expr-lang/expr"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/pkg"
	"github.com/pingcap/errors"
)

// flashbackRowsEventFunc data 不包括 event header
// Decode
func flashbackRowsEventFunc(e *RowsEvent, data []byte) error {
	//body := rawData[EventHeaderSize:]
	pos, err := e.DecodeHeader(data)
	if err != nil {
		return err
	}

	if e.dbTableFilter != nil {
		//dbMatch, _ := e.dbTableFilter.Compiled.DbFilter.MatchString(string(e.Table.Schema))
		dbTableName := fmt.Sprintf(`%s.%s`, e.Table.Schema, e.Table.Table)
		tbMatch, _ := e.dbTableFilter.Compiled.TbFilter.MatchString(dbTableName)
		if tbMatch {
			err = e.FlashbackData(pos, data)
		} else {
			e.rawBytesNew = e.rawBytesNew[:EventHeaderSize] // 会用于后面判断该事件是否有效/打印
			return nil
		}
	} else {
		err = e.FlashbackData(pos, data)
	}
	return err
}

// FlashbackData DecodeData
// pos is row event body header length
// data is event body
func (e *RowsEvent) FlashbackData(pos int, data []byte) (err2 error) {

	if e.flashback {
		return e.FlashbackData2(pos, data)
	} else {
		return e.DecodeData2(pos, data)
	}

	e.rawBytesNew = e.rawBytesNew[:EventHeaderSize+pos] // 后面的部分需要修改
	if e.compressed {
		// mariadb and tendb share the same compress algo(zlib)?
		uncompressedBuf, err3 := DecompressMariadbData(data[pos:])
		if err3 != nil {
			//nolint:nakedret
			return err3
		}
		data = append(data[:pos], uncompressedBuf...) // tendb
	}

	var (
		n   int
		err error
	)
	// ... repeat rows until event-end
	defer func() {
		if r := recover(); r != nil {
			err2 = errors.Errorf("parse rows event panic %v, data %q, parsed rows %#v, table map %#v", r, data, e, e.Table)
		}
	}()

	// Pre-allocate memory for rows: before image + (optional) after image
	rowsLen := 1
	if e.needBitmap2 {
		rowsLen++
	}
	e.SkippedColumns = make([][]int, 0, rowsLen)
	e.Rows = make([][]interface{}, 0, rowsLen)
	//var originalEventType EventType = e.eventType

	if e.flashback {
		switch e.eventType {
		case WRITE_ROWS_EVENTv1, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1:
			e.rawBytesNew[EventTypePos] = byte(DELETE_ROWS_EVENTv1)
			e.eventType = DELETE_ROWS_EVENTv1
		case WRITE_ROWS_EVENTv2, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
			e.rawBytesNew[EventTypePos] = byte(DELETE_ROWS_EVENTv2)
			e.eventType = DELETE_ROWS_EVENTv2
		case DELETE_ROWS_EVENTv1, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1:
			e.rawBytesNew[EventTypePos] = byte(WRITE_ROWS_EVENTv1)
			e.eventType = WRITE_ROWS_EVENTv1
		case DELETE_ROWS_EVENTv2, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
			e.rawBytesNew[EventTypePos] = byte(WRITE_ROWS_EVENTv2)
			e.eventType = WRITE_ROWS_EVENTv2
		case TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
			e.rawBytesNew[EventTypePos] = byte(UPDATE_ROWS_EVENTv1)
			e.eventType = UPDATE_ROWS_EVENTv1
		case TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2:
			e.rawBytesNew[EventTypePos] = byte(UPDATE_ROWS_EVENTv2)
			e.eventType = UPDATE_ROWS_EVENTv2
		default:
		}
	}
	convUpdateToWriteThis := false
	//convUpdateToWriteThis = e.convUpdateToWrite && rowImageType == EnumRowImageTypeUpdateBI
	if e.convUpdateToWrite {
		switch e.eventType {
		case UPDATE_ROWS_EVENTv2, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2:
			e.rawBytesNew[EventTypePos] = byte(WRITE_ROWS_EVENTv2)
			e.eventType = WRITE_ROWS_EVENTv2
			convUpdateToWriteThis = true
		case UPDATE_ROWS_EVENTv1, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
			e.rawBytesNew[EventTypePos] = byte(WRITE_ROWS_EVENTv1)
			e.eventType = WRITE_ROWS_EVENTv1
			convUpdateToWriteThis = true
		case UPDATE_ROWS_EVENTv0:
			e.rawBytesNew[EventTypePos] = byte(WRITE_ROWS_EVENTv0)
			e.eventType = WRITE_ROWS_EVENTv0
			convUpdateToWriteThis = true
		default:
		}
	}

	var rowImageType EnumRowImageType
	switch e.eventType {
	case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2,
		MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
		rowImageType = EnumRowImageTypeWriteAI
	case DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2,
		MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
		rowImageType = EnumRowImageTypeDeleteBI
	default:
		rowImageType = EnumRowImageTypeUpdateBI
	}

	matched := false
	if e.rowsFilter == nil {
		matched = true
	}

	columnFields := map[string]interface{}{
		"col": []interface{}{},
	}

	for pos < len(data) {
		var bufImage1 []byte
		var bufImage2 []byte

		// Parse the first image, insert AI | delete BI | update BI
		if n, err = e.decodeImage(data[pos:], e.ColumnBitmap1, rowImageType); err != nil {
			return errors.Trace(err)
		}
		bufImage1 = data[pos : pos+n]
		currentMatched := false
		if e.rowsFilter != nil {
			rowCount := len(e.Rows)
			columnFields["col"] = e.Rows[rowCount-1]
			rowMatched, _ := expr.Run(e.rowsFilter.CompiledColumnFilterExpr, columnFields)
			if res, ok := rowMatched.(bool); ok && res {
				matched = true
				currentMatched = true
				e.RowsMatched += 1
			}
		}
		if convUpdateToWriteThis && e.flashback {
			// convert 模式 + flashback, update event 取 BI
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage1...)
			} else {
				e.Rows = e.Rows[:len(e.Rows)-1]
			}
		} else if !e.flashback {
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage1...)
				e.Rows = e.Rows[:len(e.Rows)-1] // 不是 flashback 且 convUpdate, 删除 BI
			} else {
				e.Rows = e.Rows[:len(e.Rows)-1]
			}
		}
		pos += n
		e.rowsCount += 1

		// Parse the second image (for UPDATE only), update AI
		if e.needBitmap2 {
			if n, err = e.decodeImage(data[pos:], e.ColumnBitmap2, EnumRowImageTypeUpdateAI); err != nil {
				return errors.Trace(err)
			}
			bufImage2 = data[pos : pos+n]
			if convUpdateToWriteThis && !e.flashback {
				if e.rowsFilter == nil || currentMatched {
					e.rawBytesNew = append(e.rawBytesNew, bufImage2...)
				} else {
					e.Rows = e.Rows[:len(e.Rows)-1]
				}
			} else {
				if e.rowsFilter == nil || currentMatched {
					e.rawBytesNew = append(e.rawBytesNew, bufImage2...)
				} else {
					e.Rows = e.Rows[:len(e.Rows)-1]
				}
			}
			pos += n
		}
		if e.flashback && !convUpdateToWriteThis {
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage1...)
			} else {
				e.Rows = e.Rows[:len(e.Rows)-1]
			}
		}
	}
	if !matched {
		e.rawBytesNew = e.rawBytesNew[:EventHeaderSize] // 无效 event raw data
	}
	// swap rows AI/BI for update event
	if e.flashback && e.needBitmap2 && !convUpdateToWriteThis {
		rowCount := len(e.Rows)
		for i := 0; i < rowCount; i += 2 {
			tmpRow := make([]interface{}, e.ColumnCount)
			tmpRow = e.Rows[i]
			e.Rows[i] = e.Rows[i+1]
			e.Rows[i+1] = tmpRow
		}
	}
	return nil
}

// GetEventType return actual event type if flashbacked
func (e *RowsEvent) GetEventType() EventType {
	return e.eventType
}

func (e *RowsEvent) GetRowsEventPrinted(verboseLevel int) []byte {
	//var buf []byte
	w := bytes.NewBuffer(nil)
	e.PrintVerbose(w, verboseLevel)
	return w.Bytes()
}

type TableMapColumnInfo struct {
	DataType string
	Nullable bool
	//IsNull     bool
	IsBinary   bool
	isUnsigned bool
	IsNumeric  bool
	Position   int
}

func (i *TableMapColumnInfo) GetValuePrinted(v interface{}) interface{} {
	if v == nil {
		return "NULL"
	} else if i.IsBinary {
		return hex.EncodeToString(v.([]byte))
	}
	if !i.IsNumeric {
		switch v.(type) {
		case string:
			return fmt.Sprintf("'%s'", GetPrintString([]byte(v.(string))))
		case []byte:
			return fmt.Sprintf("'%s'", GetPrintString(v.([]byte)))
		default:
			return fmt.Sprintf("'%s'", v)
		}
	}

	return v
}

func GetPrintString(buf []byte) string {
	var newBuf bytes.Buffer
	for _, s := range buf {
		if s <= 31 { // 0x1F 控制字符，以 16进制输出
			newBuf.WriteString(fmt.Sprintf("\\x%02x", s))
		} else {
			newBuf.WriteByte(s)
		}
	}
	return newBuf.String()
}

func (i *TableMapColumnInfo) GetTypeString(e *TableMapEvent, pos int) string {
	meta := e.ColumnMeta[pos]
	switch e.realType(pos) {
	case MYSQL_TYPE_STRING:
		return fmt.Sprintf("STRING(%d)", meta)
	case MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING:
		return fmt.Sprintf("VARSTRING(%d)", meta)
	case MYSQL_TYPE_NULL:
		return "NULL"
	case MYSQL_TYPE_LONG:
		return "INT"
	case MYSQL_TYPE_TINY:
		return "TINYINT"
	case MYSQL_TYPE_SHORT:
		return "SHORTINT"
	case MYSQL_TYPE_INT24:
		return "MEDIUMINT"
	case MYSQL_TYPE_LONGLONG:
		return "LONGINT"
	case MYSQL_TYPE_NEWDECIMAL:
		precision := meta >> 8
		decimals := meta & 0xFF
		return fmt.Sprintf("DECIMAL(%d,%d)", precision, decimals)
	case MYSQL_TYPE_FLOAT:
		return "FLOAT"
	case MYSQL_TYPE_DOUBLE:
		return "DOUBLE"
	case MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		return fmt.Sprintf("BIT(%d)", nbits)
	case MYSQL_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case MYSQL_TYPE_TIMESTAMP2:
		return fmt.Sprintf("TIMESTAMP(%d)", meta)
	case MYSQL_TYPE_DATETIME:
		return "DATETIME"
	case MYSQL_TYPE_DATETIME2:
		return fmt.Sprintf("DATETIME(%d)", meta)
	case MYSQL_TYPE_TIME:
		return "TIME"
	case MYSQL_TYPE_TIME2:
		return fmt.Sprintf("TIME(%d)", meta)
	case MYSQL_TYPE_DATE:
		return "DATE"
	case MYSQL_TYPE_YEAR:
		return "YEAR"
	case MYSQL_TYPE_ENUM:
		bbytes := meta & 0xFF
		switch bbytes {
		case 1:
			return "ENUM(1 byte)"
		case 2:
			return "ENUM(2 bytes)"
		default:
			return "ENUM"
		}
	case MYSQL_TYPE_SET:
		return fmt.Sprintf("SET(%d bytes)", meta&0xFF)
	case MYSQL_TYPE_BLOB, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB:
		i.IsBinary = true
		switch meta {
		case 1:
			return "TINYBLOB / TINYTEXT"
		case 2:
			return "BLOB/TEXT"
		case 3:
			return "MEDIUMBLOB/MEDIUMTEXT"
		case 4:
			return "LONGBLOB/LONGTEXT"
		default:
			return "BLOB"
		}
	case MYSQL_TYPE_JSON:
		return "JSON"
	case MYSQL_TYPE_GEOMETRY:
		return "geometry"
	default:
		_ = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", e.realType(pos))
		return "unknown"
	}
}

func (e *TableMapEvent) ReadColumnInfo() {
	unsignedMap := e.UnsignedMap()

	for i := 0; i < int(e.ColumnCount); i++ {
		info := &TableMapColumnInfo{Position: i}
		info.DataType = info.GetTypeString(e, i)
		if e.IsNumericColumn(i) {
			info.IsNumeric = true
			if len(unsignedMap) > 0 && unsignedMap[i] {
				info.isUnsigned = true
			}
		}
		_, nullable := e.Nullable(i)
		if nullable {
			info.Nullable = true
		}
		e.columnsInfo = append(e.columnsInfo, info)
	}
}
func (e *RowsEvent) PrintVerbose(w io.Writer, verboseLevel int) {
	if len(e.Table.columnsInfo) == 0 {
		e.Table.ReadColumnInfo()
	}
	var sql_command, sql_clause1, sql_clause2 string
	switch e.eventType {
	case UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
		sql_command = fmt.Sprintf("### UDPATE `%s`.`%s`\n", e.Table.GetSchema(), e.Table.Table)
		for i := 0; i < len(e.Rows); i += 2 {
			sql_clause1 = "### WHERE\n"
			sql_clause2 = "### SET\n"
			e.Table.UnsignedMap()
			for k, v := range e.Rows[i] {
				sql_clause1 += fmt.Sprintf("###   col[%d]=%v", k, e.Table.columnsInfo[k].GetValuePrinted(v))
				if verboseLevel >= 2 {
					sql_clause1 += fmt.Sprintf(" /* %v %v */\n", e.Table.columnsInfo[k].DataType, e.Table.columnsInfo[k].isUnsigned)
				} else {
					sql_clause1 += fmt.Sprintf("\n")
				}
			}
			for k, v := range e.Rows[i+1] {
				sql_clause2 += fmt.Sprintf("###   col[%d]=%v", k, e.Table.columnsInfo[k].GetValuePrinted(v))
				if verboseLevel >= 2 {
					sql_clause2 += fmt.Sprintf(" /* %v %v */\n", e.Table.columnsInfo[k].DataType, e.Table.columnsInfo[k].isUnsigned)
				} else {
					sql_clause2 += fmt.Sprintf("\n")
				}
			}
			fmt.Fprintf(w, "%s%s%s", sql_command, sql_clause1, sql_clause2)
		}
	case WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2:
		sql_command = fmt.Sprintf("### INSERT INTO `%s`.`%s`\n", e.Table.GetSchema(), e.Table.Table)
		for i := 0; i < len(e.Rows); i++ {
			sql_clause1 = "### SET\n"
			sql_clause2 = ""
			for k, v := range e.Rows[i] {
				sql_clause1 += fmt.Sprintf("###   col[%d]=%v", k, e.Table.columnsInfo[k].GetValuePrinted(v))
				if verboseLevel >= 2 {
					sql_clause1 += fmt.Sprintf(" /* %v %v */\n", e.Table.columnsInfo[k].DataType, e.Table.columnsInfo[k].isUnsigned)
				} else {
					sql_clause1 += fmt.Sprintf("\n")
				}
			}
			fmt.Fprintf(w, "%s%s%s", sql_command, sql_clause1, sql_clause2)
		}
	case DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
		sql_command = fmt.Sprintf("### DELETE FROM `%s`.`%s`\n", e.Table.GetSchema(), e.Table.Table)
		for i := 0; i < len(e.Rows); i++ {
			sql_clause1 = "### WHERE\n"
			sql_clause2 = ""
			for k, v := range e.Rows[i] {
				sql_clause1 += fmt.Sprintf("###   col[%d]=%v", k, e.Table.columnsInfo[k].GetValuePrinted(v))
				if verboseLevel >= 2 {
					sql_clause1 += fmt.Sprintf(" /* %v %v */\n", e.Table.columnsInfo[k].DataType, e.Table.columnsInfo[k].isUnsigned)
				} else {
					sql_clause1 += fmt.Sprintf("\n")
				}
			}
			fmt.Fprintf(w, "%s%s%s", sql_command, sql_clause1, sql_clause2)
		}
	default:
		sql_command = ""
		sql_clause1 = ""
		sql_clause2 = ""
	}
}

func (e *TableMapEvent) SetRenameRule(rule *pkg.RenameRule) {
	e.renameRule = rule
}

func (e *TableMapEvent) GetSchema() []byte {
	if len(e.NewSchema) > 0 {
		return e.NewSchema
	}
	return e.Schema
}

// DecodeAndRename do not change original schema.table name
func (e *TableMapEvent) DecodeAndRename(data []byte) error {
	//var dataNew = make([]byte, len(data))
	//copy(dataNew, data)
	e.rawBytesNew = e.rawBytesNew[:EventHeaderSize]
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.tableIDSize])
	pos += e.tableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2
	e.rawBytesNew = append(e.rawBytesNew, data[:pos]...)

	schemaLength := data[pos]
	pos++

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	newSchema := e.renameRule.GetNewName(string(e.Schema))
	e.rawBytesNew = append(e.rawBytesNew, byte(len(newSchema)))
	e.rawBytesNew = append(e.rawBytesNew, []byte(newSchema)...)
	e.NewSchema = []byte(newSchema)

	// skip 0x00
	e.rawBytesNew = append(e.rawBytesNew, data[pos])
	pos++

	tableLength := data[pos]
	e.rawBytesNew = append(e.rawBytesNew, data[pos])
	pos++

	e.Table = data[pos : pos+int(tableLength)]
	e.rawBytesNew = append(e.rawBytesNew, e.Table...)
	pos += int(tableLength)

	// skip 0x00
	e.rawBytesNew = append(e.rawBytesNew, data[pos])
	pos++

	e.rawBytesNew = append(e.rawBytesNew, data[pos:]...)

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	e.ColumnType = data[pos : pos+int(e.ColumnCount)]
	pos += int(e.ColumnCount)

	var err error
	var metaData []byte
	if metaData, _, n, err = LengthEncodedString(data[pos:]); err != nil {
		return errors.Trace(err)
	}

	if err = e.decodeMeta(metaData); err != nil {
		return errors.Trace(err)
	}

	pos += n

	nullBitmapSize := bitmapByteSize(int(e.ColumnCount))
	if len(data[pos:]) < nullBitmapSize {
		return io.EOF
	}

	e.NullBitmap = data[pos : pos+nullBitmapSize]

	pos += nullBitmapSize

	if e.optionalMetaDecodeFunc != nil {
		if err = e.optionalMetaDecodeFunc(data[pos:]); err != nil {
			return err
		}
	} else {
		if err = e.decodeOptionalMeta(data[pos:]); err != nil {
			return err
		}
	}

	return nil
}
