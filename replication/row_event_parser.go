package replication

import (
	"encoding/binary"
	"fmt"

	"github.com/expr-lang/expr"
	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// FlashbackData2 DecodeData
// pos is row event body header length
// data is event body
func (e *RowsEvent) FlashbackData2(pos int, data []byte) (err2 error) {
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

	// flashback swap and set un-compress event
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
		bufImage1 = data[pos : pos+n] // 保存第一个镜像
		currentMatched := false
		if e.rowsFilter != nil {
			rowCount := len(e.Rows)
			columnFields["col"] = e.Rows[rowCount-1]
			rowMatched, _ := expr.Run(e.rowsFilter.CompiledColumnFilterExpr, columnFields)
			if res, ok := rowMatched.(bool); ok && res {
				currentMatched = true
				e.RowsMatched += 1
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
			// 如果是 convert 模式，后镜像不需要了
			if convUpdateToWriteThis {
				e.Rows = e.Rows[:len(e.Rows)-1]
			} else {
				if e.rowsFilter == nil || currentMatched {
					e.rawBytesNew = append(e.rawBytesNew, bufImage2...)
				} else {
					e.Rows = e.Rows[:len(e.Rows)-1]
				}
			}
			pos += n
		}
		// 无论是 convert 模式，还是 flashback 模式，如果匹配，第一个镜像都要保留
		if e.rowsFilter == nil || currentMatched {
			e.rawBytesNew = append(e.rawBytesNew, bufImage1...)
		} else {
			e.Rows = e.Rows[:len(e.Rows)-1]
		}
	}
	if e.rowsFilter != nil && e.RowsMatched == 0 {
		e.rawBytesNew = e.rawBytesNew[:EventHeaderSize] // 无效 event raw data
	}
	// swap rows AI/BI for update event
	if e.needBitmap2 && !convUpdateToWriteThis {
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

// DecodeData2 DecodeData
// pos is row event body header length
// data is event body
func (e *RowsEvent) DecodeData2(pos int, data []byte) (err2 error) {
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

	// set un-compress event
	switch e.eventType {
	case TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		e.rawBytesNew[EventTypePos] = byte(WRITE_ROWS_EVENTv1)
		e.eventType = WRITE_ROWS_EVENTv1
	case TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
		e.rawBytesNew[EventTypePos] = byte(WRITE_ROWS_EVENTv2)
		e.eventType = WRITE_ROWS_EVENTv2
	case TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		e.rawBytesNew[EventTypePos] = byte(DELETE_ROWS_EVENTv1)
		e.eventType = DELETE_ROWS_EVENTv1
	case TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
		e.rawBytesNew[EventTypePos] = byte(DELETE_ROWS_EVENTv2)
		e.eventType = DELETE_ROWS_EVENTv2
	case TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		e.rawBytesNew[EventTypePos] = byte(UPDATE_ROWS_EVENTv1)
		e.eventType = UPDATE_ROWS_EVENTv1
	case TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2:
		e.rawBytesNew[EventTypePos] = byte(UPDATE_ROWS_EVENTv2)
		e.eventType = UPDATE_ROWS_EVENTv2
	default:
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
				currentMatched = true
				e.RowsMatched += 1
			}
		}
		if convUpdateToWriteThis {
			e.Rows = e.Rows[:len(e.Rows)-1]
		} else {
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage1...)
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
			// 无论是 convert 模式，还是 正常正向解析 模式，如果匹配，第二个镜像都要保留
			if e.rowsFilter == nil || currentMatched {
				e.rawBytesNew = append(e.rawBytesNew, bufImage2...)
			} else {
				e.Rows = e.Rows[:len(e.Rows)-1]
			}
			pos += n
		}
	}
	if e.rowsFilter != nil && e.RowsMatched == 0 {
		e.rawBytesNew = e.rawBytesNew[:EventHeaderSize] // 无效 event raw data
	}
	return nil
}

// decodeValueLength only get value length, not decode bytes
func (e *RowsEvent) decodeValueLength(data []byte, tp byte, meta uint16, isPartial bool) (v interface{}, n int, err error) {
	var length = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = b0 | 0x30
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_LONG:
		n = 4
	case MYSQL_TYPE_TINY:
		n = 1
	case MYSQL_TYPE_SHORT:
		n = 2
	case MYSQL_TYPE_INT24:
		n = 3
	case MYSQL_TYPE_LONGLONG:
		n = 8
	case MYSQL_TYPE_NEWDECIMAL:
		prec := uint8(meta >> 8)
		scale := uint8(meta & 0xFF)
		v, n, err = decodeDecimal(data, int(prec), int(scale), e.useDecimal)
	case MYSQL_TYPE_FLOAT:
		n = 4
	case MYSQL_TYPE_DOUBLE:
		n = 8
	case MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		n = int(nbits+7) / 8
	case MYSQL_TYPE_TIMESTAMP:
		n = 4
	case MYSQL_TYPE_TIMESTAMP2:
		n = int(4 + (meta+1)/2)
	case MYSQL_TYPE_DATETIME:
		n = 8
	case MYSQL_TYPE_DATETIME2:
		n = int(5 + (meta+1)/2)
	case MYSQL_TYPE_TIME:
		n = 3
	case MYSQL_TYPE_TIME2:
		n = int(3 + (meta+1)/2)
	case MYSQL_TYPE_DATE:
		n = 3
	case MYSQL_TYPE_YEAR:
		n = 1
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			n = 1
		case 2:
			n = 2
		default:
			err = fmt.Errorf("Unknown ENUM packlen=%d", l)
		}
	case MYSQL_TYPE_SET:
		n = int(meta & 0xFF)
	case MYSQL_TYPE_BLOB:
		v, n, err = decodeBlobLength(data, meta)
	case MYSQL_TYPE_VARCHAR,
		MYSQL_TYPE_VAR_STRING:
		length = int(meta)
		v, n = decodeStringLength(data, length)
	case MYSQL_TYPE_STRING:
		v, n = decodeStringLength(data, length)
	case MYSQL_TYPE_JSON:
		// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java#L404
		length = int(FixedLengthInt(data[0:meta]))
		n = length + int(meta)
	case MYSQL_TYPE_GEOMETRY:
		v, n, err = decodeBlobLength(data, meta)
	default:
		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
	}

	return v, n, err
}

func decodeStringLength(data []byte, length int) (v string, n int) {
	if length < 256 {
		length = int(data[0])

		n = length + 1
	} else {
		length = int(binary.LittleEndian.Uint16(data[0:]))
		n = length + 2
	}
	return
}

func decodeBlobLength(data []byte, meta uint16) (v []byte, n int, err error) {
	var length int
	switch meta {
	case 1:
		length = int(data[0])
		n = length + 1
	case 2:
		length = int(binary.LittleEndian.Uint16(data))
		n = length + 2
	case 3:
		length = int(FixedLengthInt(data[0:3]))
		n = length + 3
	case 4:
		length = int(binary.LittleEndian.Uint32(data))
		n = length + 4
	default:
		err = fmt.Errorf("invalid blob packlen = %d", meta)
	}
	return
}
