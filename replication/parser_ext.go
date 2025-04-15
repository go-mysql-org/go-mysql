package replication

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/go-mysql-org/go-mysql/pkg"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type PrintEventInfo struct {
	footerBuf         *bytes.Buffer
	tableMapRawBase64 map[uint64][]byte

	begin                   string
	commit                  string
	short                   bool
	idempotent              bool
	disableLogBin           bool
	disableForeignKeyChecks bool
	autocommit              bool
	delimiter               string
	rowsStart               string
	rowsEnd                 string
	verboseLevel            int
	// The X in --base64-output=X
	base64OutputMode         string
	verbose                  uint8
	sql_mode_inited          bool
	sql_mode                 string
	charset_inited           bool
	charset                  string
	thread_id_printed        bool
	thread_id                uint64
	flags2_inited            bool
	flags2                   uint32
	auto_increment_increment uint64
	auto_increment_offset    uint64

	// True if the --skip-gtids flag was specified.
	skip_gtids bool
	/* printed_fd_event
	   This is set whenever a Format_description_event is printed.
	   Later, when an event is printed in base64, this flag is tested: if
	   no Format_description_event has been seen, it is unsafe to print
	   the base64 event, so an error message is generated.
	*/
	fd_event_printed  bool
	table_map         map[int]string
	table_map_ignored map[int]string
	/*
	   These three caches are used by the row-based replication events to
	   collect the header information and the main body of the events
	   making up a statement and in footer section any verbose related details
	   or comments related to the statment.
	*/
	headCache   io.Writer
	bodyCache   io.Writer
	footerCache io.Writer
	// Indicate if the body cache has unflushed events
	have_unflushed_events bool
	/* skipped_event_in_transaction
	   True if an event was skipped while printing the events of
	   a transaction and no COMMIT statement or XID event was ever
	   output (ie, was filtered out as well). This can be triggered
	   by the --database option of mysqlbinlog.

	   False, otherwise.
	*/
	skipped_event_in_transaction bool

	// rows_filter
	// event_filter
}

func (i *PrintEventInfo) Init() {
	i.short = viper.GetBool("short")
	i.idempotent = viper.GetBool("idempotent")
	i.disableLogBin = viper.GetBool("disable-log-bin")
	i.disableForeignKeyChecks = viper.GetBool("disable-foreign-key-checks")
	i.autocommit = viper.GetBool("autocommit")
	i.charset = viper.GetString("set-charset")
	i.verboseLevel = viper.GetInt("verbose")
	i.delimiter = "/*!*/;"
	i.rowsStart = "\nBINLOG '"
	i.rowsEnd = "'" + i.delimiter
	i.begin = "BEGIN"
	i.commit = "COMMIT"
	i.tableMapRawBase64 = make(map[uint64][]byte)
}

// ParseFileAndPrint 解析1个 binlog文件，并打印输出到 1 个目标 io (file/stdout)
func (p *BinlogParser) ParseFileAndPrint(fileName string, resultFileName string) (err error) {
	var outputWriter io.WriteCloser
	if resultFileName != "" {
		outputWriter, err = os.OpenFile(resultFileName, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	} else {
		outputWriter = os.Stdout
	}
	var ioWriter pkg.BinlogWriter
	if p.Flashback {
		cwd, _ := os.Getwd()
		cacheSize := viper.GetInt("result-file-max-size-mb") / 2 * 1024 * 1024
		cacheFilePrefix := filepath.Join(cwd, filepath.Base(fileName))
		ioWriter = pkg.NewFlashbackWriter(cacheFilePrefix, cacheSize, outputWriter)
	} else {
		ioWriter = pkg.NewNormalWriter(outputWriter)
	}

	f := func(e *BinlogEvent) error {
		//enc := b64.NewEncoder(b64.StdEncoding, ioWriter)
		switch e.Header.EventType {
		case FORMAT_DESCRIPTION_EVENT:
			r := e.Event.(*FormatDescriptionEvent)
			buf := bytes.NewBuffer(nil)
			buf.WriteString("/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;\n")
			if p.disableLogBin {
				buf.WriteString("/*!32316 SET @OLD_SQL_LOG_BIN=@@SQL_LOG_BIN, SQL_LOG_BIN=0*/;\n")
			}
			if p.disableForeignKeyChecks {
				buf.WriteString("/*!32316 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0*/;\n")
			}
			buf.WriteString("/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;\n")
			if p.idempotent {
				buf.WriteString("/*!50700 SET @@SESSION.RBR_EXEC_MODE=IDEMPOTENT*/;\n")
			}
			if p.charset != "" {
				buf.WriteString("\n/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;")
				buf.WriteString("\n/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;")
				buf.WriteString("\n/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;")
				buf.WriteString(fmt.Sprintf("\n/*!40101 SET NAMES %s */;\n", p.charset))
			}
			if p.autocommit {
				buf.WriteString("/*!50003 SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT,AUTOCOMMIT=1*/;\n")
			}
			buf.WriteString("\nDELIMITER " + p.delimiter + "\n")

			// FormatDescriptionEvent
			buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
			buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Version=%d",
				unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, r.Version) + "\n")
			buf.WriteString(p.rowsStart + "\n")
			buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
			buf.WriteString(p.rowsEnd + "\n")

			if p.Flashback {
				ioWriter.SetHeader(buf.Bytes())
			} else {
				ioWriter.Write(buf.Bytes())
			}
		case FAKE_DONE_EVENT:
			if p.footerBuf == nil { // no rotate event found
				buf := bytes.NewBuffer(nil)
				buf.WriteString("\nDELIMITER ;\n")
				buf.WriteString("# End of log file\n")
				buf.WriteString(fmt.Sprintf("# No rotate_event or stop_event found\n"))

				buf.WriteString("/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;\n")
				if p.disableLogBin {
					buf.WriteString("/*!32316 SET SQL_LOG_BIN=@OLD_SQL_LOG_BIN*/;\n")
				}
				if p.disableForeignKeyChecks {
					buf.WriteString("/*!32316 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS*/;\n")
				}
				buf.WriteString("/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;\n")
				if p.idempotent {
					buf.WriteString("/*!50700 SET @@SESSION.RBR_EXEC_MODE=STRICT*/;\n")
				}
				if p.autocommit {
					buf.WriteString("/*!50003 SET AUTOCOMMIT=@OLD_AUTOCOMMIT*/;\n")
				}
				if p.charset != "" {
					buf.WriteString("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n")
					buf.WriteString("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n")
					buf.WriteString("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n")
				}
				p.footerBuf = buf
			}
			if p.Flashback {
				ioWriter.SetFooter(p.footerBuf.Bytes())
			} else {
				ioWriter.Write(p.footerBuf.Bytes())
			}
		case ROTATE_EVENT: // STOP_EVENT
			r := e.Event.(*RotateEvent)
			buf := bytes.NewBuffer(nil)
			buf.WriteString("\nDELIMITER ;\n")
			buf.WriteString("# End of log file\n")
			buf.WriteString(fmt.Sprintf("# Next %s\n", r.NextLogName))

			buf.WriteString("/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;\n")
			if p.disableLogBin {
				buf.WriteString("/*!32316 SET SQL_LOG_BIN=@OLD_SQL_LOG_BIN*/;\n")
			}
			if p.disableForeignKeyChecks {
				buf.WriteString("/*!32316 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS*/;\n")
			}
			buf.WriteString("/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;\n")
			if p.idempotent {
				buf.WriteString("/*!50700 SET @@SESSION.RBR_EXEC_MODE=STRICT*/;\n")
			}
			if p.autocommit {
				buf.WriteString("/*!50003 SET AUTOCOMMIT=@OLD_AUTOCOMMIT*/;\n")
			}
			if p.charset != "" {
				buf.WriteString("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n")
				buf.WriteString("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n")
				buf.WriteString("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n")
			}
			p.footerBuf = buf

		case TABLE_MAP_EVENT:
			r := e.Event.(*TableMapEvent)
			buf := bytes.NewBuffer(nil)
			p.tableMapRawBase64[r.TableID] = pkg.Base64EncodeToFixedBytes(e.RawData)
			//if !p.short {
			buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
			buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
				unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(),
				e.Header.LogPos, r.Schema, r.Table, r.TableID) + "\n")
			ioWriter.Write(buf.Bytes())
			//}
		case WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1, TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
			r := e.Event.(*RowsEvent)
			buf := bytes.NewBuffer(nil)

			if len(e.RawData) <= EventHeaderSize {
				//fmt.Println("xxxxx insert", "not matched")
				if !p.short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
						e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				}
			} else {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d Rows=%d/%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID, r.RowsMatched, r.rowsCount) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", p.begin, p.delimiter))

				buf.WriteString(p.rowsStart + "\n")
				buf.Write(p.tableMapRawBase64[r.TableID])
				buf.WriteString("\n")
				buf.Write(pkg.Base64EncodeToFixedBytes(e.RawData))
				//buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString("\n" + p.rowsEnd + "\n")
				if p.verboseLevel > 0 {
					buf.Write(r.GetRowsEventPrinted(p.verboseLevel))
				}
				buf.WriteString(fmt.Sprintf("%s%s\n", p.commit, p.delimiter))
			}
			ioWriter.Write(buf.Bytes())
		case DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1, TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
			buf := bytes.NewBuffer(nil)
			r := e.Event.(*RowsEvent)

			if len(e.RawData) <= EventHeaderSize {
				//fmt.Println("xxxxx delete", "not matched")
				if !p.short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
						e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				}
			} else {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d Rows=%d/%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID, r.RowsMatched, r.rowsCount) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", p.begin, p.delimiter))

				buf.WriteString(p.rowsStart + "\n")
				buf.Write(p.tableMapRawBase64[r.TableID])
				buf.WriteString("\n")
				buf.Write(pkg.Base64EncodeToFixedBytes(e.RawData))
				//buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString("\n" + p.rowsEnd + "\n")
				if p.verboseLevel > 0 {
					buf.Write(r.GetRowsEventPrinted(p.verboseLevel))
				}
				buf.WriteString(fmt.Sprintf("%s%s\n", p.commit, p.delimiter))
			}
			ioWriter.Write(buf.Bytes())
		case UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V1, TENDB_UPDATE_ROWS_COMPRESSED_EVENT_V2:
			buf := bytes.NewBuffer(nil)
			r := e.Event.(*RowsEvent)

			if len(e.RawData) <= EventHeaderSize {
				//fmt.Println("xxxxx update", "not matched")
				if !p.short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
						e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				}
			} else {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d Rows=%d/%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID, r.RowsMatched, r.rowsCount) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", p.begin, p.delimiter))

				buf.WriteString(p.rowsStart + "\n")
				buf.Write(p.tableMapRawBase64[r.TableID])
				buf.WriteString("\n")
				buf.Write(pkg.Base64EncodeToFixedBytes(e.RawData))
				//buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString("\n" + p.rowsEnd + "\n")
				if p.verboseLevel > 0 {
					buf.Write(r.GetRowsEventPrinted(p.verboseLevel))
				}
				buf.WriteString(fmt.Sprintf("%s%s\n", p.commit, p.delimiter))
			}
			ioWriter.Write(buf.Bytes())

		case QUERY_EVENT:
			qe := e.Event.(*QueryEvent)
			buf := bytes.NewBuffer(nil)
			if p.Flashback && qe.DbTableMatched {
				return errors.Errorf("statement error: %s", qe.Query)
			} else if p.TableFilter == nil || (p.TableFilter != nil && qe.DbTableMatched) {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, qe.Schema) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", p.begin, p.delimiter))

				if string(qe.Schema) != "" {
					buf.WriteString(fmt.Sprintf("USE `%s`%s\n", qe.Schema, p.delimiter))
				}
				buf.WriteString(fmt.Sprintf("SET TIMESTAMP=%d%s\n", e.Header.Timestamp, p.delimiter))
				buf.Write(qe.Query)
				buf.WriteString("\n" + p.delimiter + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", p.commit, p.delimiter))
			} else {
				if !p.short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, qe.Schema) + "\n")
				}
				// 不打印 statement
			}
			ioWriter.Write(buf.Bytes())
		default:
			if !p.short {
				buf := bytes.NewBuffer(nil)
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos) + "\n")
				ioWriter.Write(buf.Bytes())
			}
		}
		return nil
	}
	// p.TimeFilter.StopPos 边界判断在 ParseFile 里面
	if err = p.ParseFile(fileName, int64(p.TimeFilter.StartPos), f); err != nil {
		_ = ioWriter.Close()
		return err
	}
	return ioWriter.Close()
}

func unixTimeToStr(ts uint32) string {
	return time.Unix(int64(ts), 0).Local().Format(time.DateTime)
}

// RowsFilter binlog rows filter
type RowsFilter struct {
	// columnFilterExpr go expression evaluation
	columnFilterExpr string
	// CompiledColumnFilterExpr compiled go-expr
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
	//fmt.Println("xxxx", rf.columnFilterExpr)
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

// ParseEvent2 change rawData
// rawData is a new object
func (p *BinlogParser) ParseEvent2(h *EventHeader, data []byte, rawData *[]byte) (Event, []byte, error) {
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
		if p.RowsFilter != nil || p.verboseLevel > 0 {
			re.printValueMeta = true // 需要 decode row values
		}
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
