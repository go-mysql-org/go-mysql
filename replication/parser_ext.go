package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"slices"

	"github.com/dlclark/regexp2"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pkg/errors"
)

type RowsFilter struct {
	columnFilterExpr         string
	CompiledColumnFilterExpr *vm.Program
	// rowsMatch rows filter matched records number in this rows event
	rowsMatch int
}

type TimeFilter struct {
	StartTime uint32
	StopTime  uint32
	StartPos  uint32
	StopPos   uint32
}

// NewRowsFilter return compile expr
func NewRowsFilter(filterExpr string) (*RowsFilter, error) {
	rf := &RowsFilter{
		columnFilterExpr: filterExpr,
	}
	if err := rf.compile(); err != nil {
		return nil, err
	}
	return rf, nil
}

func (rf *RowsFilter) compile() error {
	columnFields := map[string]interface{}{
		"col": []interface{}{},
	}
	program, err := expr.Compile(rf.columnFilterExpr, expr.Env(columnFields))
	if err != nil {
		return errors.WithMessage(err, "parse rows filter expression")
	}
	rf.CompiledColumnFilterExpr = program
	return nil
}

type TableFilterRegex struct {
	DbFilter *regexp2.Regexp
	TbFilter *regexp2.Regexp
}

// swapFlashbackEventType do not need handle compressed event
func (p *BinlogParser) swapFlashbackEventType(h *EventHeader) {
	// compressed 事件，会被解压成对应的 row 解压后的事件
	switch h.EventType {
	case WRITE_ROWS_EVENTv2:
		h.EventType = DELETE_ROWS_EVENTv2
	case WRITE_ROWS_EVENTv1:
		h.EventType = DELETE_ROWS_EVENTv1
	case WRITE_ROWS_EVENTv0:
		h.EventType = DELETE_ROWS_EVENTv0
	case DELETE_ROWS_EVENTv2:
		h.EventType = WRITE_ROWS_EVENTv2
	case DELETE_ROWS_EVENTv1:
		h.EventType = WRITE_ROWS_EVENTv1
	case DELETE_ROWS_EVENTv0:
		h.EventType = WRITE_ROWS_EVENTv0
	case TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
		h.EventType = TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2
	case TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		h.EventType = TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1
	case TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
		h.EventType = TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2
	case TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		h.EventType = TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1
	default:
	}
}

var (
	UpdateEventType = []EventType{UPDATE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv0, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1}
	DeleteEventType = []EventType{DELETE_ROWS_EVENTv2, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv0, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1}
	InsertEventType = []EventType{WRITE_ROWS_EVENTv2, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv0, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1}
)

// ParseEventFlashback change rawData
// rawData is a new object
func (p *BinlogParser) ParseEventFlashback(h *EventHeader, data []byte, rawData *[]byte) (Event, []byte, error) {
	var e Event

	if h.EventType == FORMAT_DESCRIPTION_EVENT {
		p.format = &FormatDescriptionEvent{}
		e = p.format
	} else {
		if p.format != nil && p.format.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
			err := p.verifyCrc32Checksum(*rawData)
			if err != nil {
				return nil, nil, err
			}
			data = data[0 : len(data)-BinlogChecksumLength]
		}

		if h.EventType == ROTATE_EVENT {
			e = &RotateEvent{}
		} else if !p.rawMode {
			switch h.EventType {
			case QUERY_EVENT:
				e = &QueryEvent{}
			case MARIADB_QUERY_COMPRESSED_EVENT:
				e = &QueryEvent{
					compressed: true,
				}
			case XID_EVENT:
				e = &XIDEvent{}
			case TABLE_MAP_EVENT:
				te := &TableMapEvent{
					flavor:                 p.flavor,
					optionalMetaDecodeFunc: p.tableMapOptionalMetaDecodeFunc,
				}
				if p.format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
					te.tableIDSize = 4
				} else {
					te.tableIDSize = 6
				}
				e = te
			case TENDB_QUERY_COMPRESSED_EVENT: // todo
				e = &QueryEvent{
					compressed: true,
				}
			case WRITE_ROWS_EVENTv0,
				UPDATE_ROWS_EVENTv0,
				DELETE_ROWS_EVENTv0,
				WRITE_ROWS_EVENTv1,
				DELETE_ROWS_EVENTv1,
				UPDATE_ROWS_EVENTv1,
				WRITE_ROWS_EVENTv2,
				UPDATE_ROWS_EVENTv2,
				DELETE_ROWS_EVENTv2,
				MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1,
				MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1,
				MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1,
				TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1,
				TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1,
				TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1,
				TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2,
				TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2,
				TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2,
				PARTIAL_UPDATE_ROWS_EVENT: // Extension of UPDATE_ROWS_EVENT, allowing partial values according to binlog_row_value_options
				if p.EventTypeFilter != nil {
					if !slices.Contains(p.EventTypeFilter, h.EventType) {
						//filterMatched := false
						//e = &GenericEvent{FilterMatched: &filterMatched}
						re := p.newRowsEvent(h)
						re.RowsMatched = 0
						rawBytesNew := (*rawData)[:EventHeaderSize]
						e = re
						return e, rawBytesNew, nil
					}
				}
				e = p.newRowsEvent(h) // 也可以考虑在 newRowsEvent 里面 swap type
			case ROWS_QUERY_EVENT:
				e = &RowsQueryEvent{}
			case GTID_EVENT:
				e = &GTIDEvent{}
			case ANONYMOUS_GTID_EVENT:
				e = &GTIDEvent{}
			case BEGIN_LOAD_QUERY_EVENT:
				e = &BeginLoadQueryEvent{}
			case EXECUTE_LOAD_QUERY_EVENT:
				e = &ExecuteLoadQueryEvent{}
			case MARIADB_ANNOTATE_ROWS_EVENT:
				e = &MariadbAnnotateRowsEvent{}
			case MARIADB_BINLOG_CHECKPOINT_EVENT:
				e = &MariadbBinlogCheckPointEvent{}
			case MARIADB_GTID_LIST_EVENT:
				e = &MariadbGTIDListEvent{}
			case MARIADB_GTID_EVENT:
				ee := &MariadbGTIDEvent{}
				ee.GTID.ServerID = h.ServerID
				e = ee
			case PREVIOUS_GTIDS_EVENT:
				e = &PreviousGTIDsEvent{}
			case INTVAR_EVENT:
				e = &IntVarEvent{}
			case TRANSACTION_PAYLOAD_EVENT:
				e = p.newTransactionPayloadEvent()
			default:
				e = &GenericEvent{}
			}
		} else {
			e = &GenericEvent{}
		}
	}

	var err error

	if re, ok := e.(*RowsEvent); ok {
		re.SetDbTableFilter(p.TableFilter)
		re.SetRowsFilter(p.RowsFilter)
		re.flashback = p.Flashback
		re.convUpdateToWrite = p.ConvUpdateToWrite
		if p.rowsEventDecodeFunc != nil {
			re.rawBytesNew = *rawData
			err = p.rowsEventDecodeFunc(re, data) // todo handle err?
			if len(re.rawBytesNew) > EventHeaderSize {
				if p.format != nil && p.format.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
					re.rawBytesNew = append(re.rawBytesNew, p.computeCrc32Checksum(re.rawBytesNew)...)
				}
				eventSizeBuff := make([]byte, 4)
				binary.LittleEndian.PutUint32(eventSizeBuff, uint32(len(re.rawBytesNew)))
				BytesReplaceWithIndex(re.rawBytesNew, EventSizPos, EventSizPos+4, eventSizeBuff)
			}
			rawData = &re.rawBytesNew
		} else {
			err = e.Decode(data)
		}
	} else if qe, ok := e.(*QueryEvent); ok {
		err = e.Decode(data)
		if p.Flashback || p.TableFilter != nil || p.RowsFilter != nil {
			if bytes.Equal(qe.Query, []byte("BEGIN")) || bytes.Equal(qe.Query, []byte("COMMIT")) {
				//fmt.Fprintf(iowriter, "%s%s\n", r.Query, Delimiter)
			}
			sqlParser := parser.New()
			stmts, _, err := sqlParser.Parse(string(qe.Query), "", "")
			if err != nil {
				fmt.Printf("parse query(%s) err %v, will skip this event\n", qe.Query, err)
				//return nil
			}
			for _, stmt := range stmts {
				nodes := ParseStmt(stmt)
				for _, node := range nodes {
					if node.Schema == "" {
						node.Schema = string(qe.Schema)
					}
					qe.dbTable = nodes
					//fmt.Printf("parsed table name <%s.%s>\n", node.Schema, node.Table)
					dbTableName := fmt.Sprintf(`%s.%s`, node.Schema, node.Table)
					tbMatch, _ := p.TableFilter.Compiled.TbFilter.MatchString(dbTableName)
					if tbMatch {
						qe.DbTableMatched = true
						if p.Flashback {
							return nil, nil, errors.Errorf("flashback rows found statement [%s] table matched: [%s]",
								qe.Query, dbTableName)
						}
					}
				}
			}
		}
	} else if te, ok := e.(*TableMapEvent); ok {
		te.SetRenameRule(p.RenameRule)
		if te.renameRule != nil {
			te.rawBytesNew = *rawData
			err = te.DecodeAndRename(data)
			if p.format != nil && p.format.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
				te.rawBytesNew = append(te.rawBytesNew, p.computeCrc32Checksum(te.rawBytesNew)...)
			}
			eventSizeBuff := make([]byte, 4)
			binary.LittleEndian.PutUint32(eventSizeBuff, uint32(len(te.rawBytesNew)))
			BytesReplaceWithIndex(te.rawBytesNew, EventSizPos, EventSizPos+4, eventSizeBuff)
			rawData = &te.rawBytesNew
		} else {
			err = e.Decode(data)
		}
	} else {
		err = e.Decode(data)
	}

	if err != nil {
		return nil, nil, &EventError{h, err.Error(), data}
	}

	if te, ok := e.(*TableMapEvent); ok {
		p.tables[te.TableID] = te
	}

	if re, ok := e.(*RowsEvent); ok {
		if (re.Flags & RowsEventStmtEndFlag) > 0 {
			// Refer https://github.com/alibaba/canal/blob/38cc81b7dab29b51371096fb6763ca3a8432ffee/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogEvent.java#L176
			p.tables = make(map[uint64]*TableMapEvent)
		}
	}

	return e, *rawData, nil
}

func (p *BinlogParser) computeCrc32Checksum(rawData []byte) []byte {
	/*
		if !p.verifyChecksum {
			rawData[EventSizPos] = byte(len(rawData))
			return nil
		}
	*/
	calculatedPart := rawData

	// mysql use zlib's CRC32 implementation, which uses polynomial 0xedb88320UL.
	// reference: https://github.com/madler/zlib/blob/master/crc32.c
	// https://github.com/madler/zlib/blob/master/doc/rfc1952.txt#L419
	checksum := crc32.ChecksumIEEE(calculatedPart)
	computed := make([]byte, BinlogChecksumLength)
	binary.LittleEndian.PutUint32(computed, checksum)
	return computed
}

type SchemaNode struct {
	Schema string
	Table  string
}

// alter table add column xxx  -> alter table drop column xxx
// alter table add index, drop index -> ok
// drop table, truncate table, alter table modify column xxx, drop column -> no

func ParseStmt(stmt ast.StmtNode) (ns []*SchemaNode) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		ns = make([]*SchemaNode, len(t.TableToTables))
		for i, tableInfo := range t.TableToTables {
			ns[i] = &SchemaNode{
				Schema: tableInfo.OldTable.Schema.String(),
				Table:  tableInfo.OldTable.Name.String(),
			}
		}
	case *ast.AlterTableStmt:
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
		if len(t.Specs) > 0 {
			for _, spec := range t.Specs {
				if spec.Tp == ast.AlterTableAddColumns {
					//fmt.Println("AlterTableAddColumns", spec.Name, spec.Text())
				} else if spec.Tp == ast.AlterTableDropIndex {
					//fmt.Println("AlterTableDropIndex", spec.IndexName, spec.Text())
				} else if spec.Tp == ast.AlterTableAddConstraint {
					//fmt.Println("AlterTableAddConstraint-Index", spec.IndexName, spec.OriginalText())
				} else {
					//fmt.Println("AlterTableStmt-XX", spec.Tp, spec.OriginalText())
				}
			}
		}
	case *ast.DropTableStmt:
		ns = make([]*SchemaNode, len(t.Tables))
		for i, table := range t.Tables {
			ns[i] = &SchemaNode{
				Schema: table.Schema.String(),
				Table:  table.Name.String(),
			}
		}
	case *ast.CreateTableStmt:
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.TruncateTableStmt:
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.CreateIndexStmt:
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.DropIndexStmt:
		n := &SchemaNode{
			Schema: t.Table.Schema.String(),
			Table:  t.Table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.InsertStmt:
		tableSource := t.Table.TableRefs.Left.(*ast.TableSource)
		table, ok := tableSource.Source.(*ast.TableName)
		if !ok || table == nil {
			return nil
		}
		n := &SchemaNode{
			Schema: table.Schema.String(),
			Table:  table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.DeleteStmt:
		tableSource := t.TableRefs.TableRefs.Left.(*ast.TableSource)
		table, ok := tableSource.Source.(*ast.TableName)
		if !ok || table == nil {
			return nil
		}
		//GetTables()
		n := &SchemaNode{
			Schema: table.Schema.String(),
			Table:  table.Name.String(),
		}
		ns = []*SchemaNode{n}
	case *ast.UpdateStmt:
		tableSource := t.TableRefs.TableRefs.Left.(*ast.TableSource)
		table, ok := tableSource.Source.(*ast.TableName)
		if !ok || table == nil {
			return nil
		}
		n := &SchemaNode{
			Schema: table.Schema.String(),
			Table:  table.Name.String(),
		}
		ns = []*SchemaNode{n}
	}
	return ns
}
