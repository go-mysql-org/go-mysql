package replication

import (
	"fmt"
	"io"

	"github.com/expr-lang/expr"
	. "github.com/go-mysql-org/go-mysql/mysql"
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

	var rowImageType EnumRowImageType
	switch e.eventType { // flashback 这里已经swap了
	case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2,
		MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
		rowImageType = EnumRowImageTypeWriteAI
	case DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2,
		MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
		rowImageType = EnumRowImageTypeDeleteBI
	default:
		rowImageType = EnumRowImageTypeUpdateBI
	}

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

		// Parse the first image
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
			} else {
				e.Rows = e.Rows[:len(e.Rows)-1]
			}
		}
		if !e.flashback {
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage1...)
			}
		}
		pos += n

		// Parse the second image (for UPDATE only)
		if e.needBitmap2 {
			if n, err = e.decodeImage(data[pos:], e.ColumnBitmap2, EnumRowImageTypeUpdateAI); err != nil {
				return errors.Trace(err)
			}
			bufImage2 = data[pos : pos+n]
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage2...)
			} else if e.rowsFilter != nil && !currentMatched {
				e.Rows = e.Rows[:len(e.Rows)-1]
			}
			pos += n
		}
		if e.flashback {
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage1...)
			}
		}
	}
	if !matched {
		e.rawBytesNew = e.rawBytesNew[:EventHeaderSize] // 无效 event raw data
	}
	// swap rows AI/BI for update event
	if e.needBitmap2 {
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

func (e *RowsEvent) PrintVerbose(w io.Writer) {
	var sql_command, sql_clause1, sql_clause2 string
	switch e.eventType {
	case UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
		sql_command = fmt.Sprintf("### UDPATE `%s`.`%s`\n", e.Table.Schema, e.Table.Table)
		for i := 0; i < len(e.Rows); i += 2 {
			sql_clause1 = "### WHERE\n"
			sql_clause2 = "### SET\n"
			for k, v := range e.Rows[i] {
				sql_clause1 += fmt.Sprintf("###   @%d=%v\n", k+1, v)
			}
			for k, v := range e.Rows[i+1] {
				sql_clause2 += fmt.Sprintf("###   @%d=%v\n", k+1, v)
			}
			fmt.Fprintf(w, "%s%s%s", sql_command, sql_clause1, sql_clause2)
		}
	case WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2:
		sql_command = fmt.Sprintf("### INSERT INTO `%s`.`%s`\n", e.Table.Schema, e.Table.Table)
		for i := 0; i < len(e.Rows); i++ {
			sql_clause1 = "### SET\n"
			sql_clause2 = ""
			for k, v := range e.Rows[i] {
				sql_clause1 += fmt.Sprintf("###   @%d=%v\n", k+1, v)
			}
			fmt.Fprintf(w, "%s%s%s", sql_command, sql_clause1, sql_clause2)
		}
	case DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
		sql_command = fmt.Sprintf("### DELETE FROM `%s`.`%s`\n", e.Table.Schema, e.Table.Table)
		for i := 0; i < len(e.Rows); i++ {
			sql_clause1 = "### WHERE\n"
			sql_clause2 = ""
			for k, v := range e.Rows[i] {
				if e.Table.ColumnType[k] < MYSQL_TYPE_DOUBLE { // todo
					sql_clause1 += fmt.Sprintf("###   @%d=%d\n", k+1, v)
				} else {
					sql_clause1 += fmt.Sprintf("###   @%d=%s\n", k+1, v)
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
