package main

import (
	"bytes"
	b64 "encoding/base64"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/pkg"
	"github.com/go-mysql-org/go-mysql/pkg/db_table_filter"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/spf13/viper"
)

type PrintEventInfo struct {
	delimiter string
	rowsStart string
	rowsEnd   string
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

type Row2EventPrinter struct {
	BinlogName  string // like binlog3306.00001
	TableName   string // db.table
	ChunkSize   int    // 128MB default
	ChunkId     int    // from 000 to 999
	EventPartId string // from zzz to aaa
	FileName    string // BinlogName.TableName.ChunkId.EventPartId.sql

	EventBody map[string][]byte // key is event part id that range from zzz to aaa
}

type ParallelType string

const (
	TypeDefault      = "" // default
	TypeDatabase     = "database"
	TypeTable        = "table"
	TypeTableHash    = "table_hash"
	TypePrimaryHash  = "primary_hash"
	TypeLogicalClock = "logical_clock"
)

func unixTimeToStr(ts uint32) string {
	return time.Unix(int64(ts), 0).Local().Format(time.DateTime)
}

func parseBinlogFile() error {
	BEGIN := "BEGIN"
	COMMIT := "COMMIT"
	Delimiter := "/*!*/;"

	//var tableMaps = map[uint64]*replication.TableMapEvent{}
	tableMapRawbytes := make(map[uint64][]byte)
	tableMapRawBase64 := make(map[uint64]string)

	p := replication.NewBinlogParser()
	p.Flashback = viper.GetBool("flashback")
	p.ConvUpdateToWrite = viper.GetBool("conv-rows-update-to-write")
	eventTypeFilter := viper.GetStringSlice("rows-event-type")
	if len(eventTypeFilter) > 0 {
		for _, evt := range eventTypeFilter {
			if evt == "delete" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.DeleteEventType...)
			} else if evt == "update" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.UpdateEventType...)
			} else if evt == "insert" {
				p.EventTypeFilter = append(p.EventTypeFilter, replication.InsertEventType...)
			} else {
				return errors.Errorf("unknown eventy type %s", evt)
			}
		}
	}

	renameRules := viper.GetStringSlice("rewrite-db")
	if len(renameRules) > 0 {
		if rules, err := pkg.NewRenameRule(renameRules); err != nil {
			return err
		} else {
			p.RenameRule = rules
		}
	}

	timeFilter := &replication.TimeFilter{
		StartPos: viper.GetUint32("start-position"),
		StopPos:  viper.GetUint32("stop-position"),
	}
	fileName := viper.GetString("file")
	if start := viper.GetString("start-datetime"); start != "" {
		startDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("start-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse start-datetime")
		}
		timeFilter.StartTime = uint32(startDatetime.Local().Unix())
	}
	if stop := viper.GetString("stop-datetime"); stop != "" {
		stopDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("stop-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse stop-datetime")
		}
		timeFilter.StopTime = uint32(stopDatetime.Local().Unix())
	}
	verbose := viper.GetBool("verbose")
	short := viper.GetBool("short")
	idempotent := viper.GetBool("idempotent")
	disableLogBin := viper.GetBool("disable-log-bin")
	autocommit := viper.GetBool("autocommit")
	charset := viper.GetString("set-charset")

	//printParser := replication.NewBinlogParser()
	//defer fileCache.Close()
	//cacheWriter := bufio.NewWriterSize(fileCache, 128*1024*1024*1024)

	/*
		ioWriter, err2 := os.OpenFile("/data/workspace/go/src/sync/go-mysql/ft_local/parsed_binlog_test.txt", os.O_TRUNC|os.O_WRONLY|os.O_CREATE, os.ModePerm)
		if err2 != nil {
			return err2
		}
		defer ioWriter.Close()
	*/
	var ioWriter BinlogWriter
	if p.Flashback {
		ioWriter = NewFlashbackWriter(fileName, viper.GetInt("result-file-max-size-mb")/2*1024*1024)
	} else {
		ioWriter = NewNormalWriter(os.Stdout)
	}
	defer ioWriter.Close()
	rowsStart := "\nBINLOG '"
	rowsEnd := "'" + Delimiter

	f := func(e *replication.BinlogEvent) error {

		//enc := b64.NewEncoder(b64.StdEncoding, ioWriter)
		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			r := e.Event.(*replication.FormatDescriptionEvent)
			buf := bytes.NewBuffer(nil)
			buf.WriteString("/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;\n")
			if disableLogBin {
				buf.WriteString("/*!32316 SET @OLD_SQL_LOG_BIN=@@SQL_LOG_BIN, SQL_LOG_BIN=0*/;\n")
			}
			buf.WriteString("/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;\n")
			if idempotent {
				buf.WriteString("/*!50700 SET @@SESSION.RBR_EXEC_MODE=IDEMPOTENT*/;\n")
			}
			if charset != "" {
				buf.WriteString("\n/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;")
				buf.WriteString("\n/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;")
				buf.WriteString("\n/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;")
				buf.WriteString(fmt.Sprintf("\n/*!40101 SET NAMES %s */;\n", charset))
			}
			if autocommit {
				buf.WriteString("/*!50003 SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT,AUTOCOMMIT=1*/;\n")
			}
			buf.WriteString("\nDELIMITER " + Delimiter + "\n")

			// FormatDescriptionEvent
			buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
			buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Version=%d",
				unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, r.Version) + "\n")
			buf.WriteString(rowsStart + "\n")
			buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
			buf.WriteString(rowsEnd + "\n")

			if p.Flashback {
				ioWriter.SetHeader(buf.Bytes())
			} else {
				ioWriter.Write(buf.Bytes())
			}
		case replication.ROTATE_EVENT: // STOP_EVENT
			r := e.Event.(*replication.RotateEvent)
			buf := bytes.NewBuffer(nil)
			buf.WriteString("\nDELIMITER ;\n")
			buf.WriteString("# End of log file\n")
			buf.WriteString(fmt.Sprintf("# Next %s\n", r.NextLogName))

			buf.WriteString("/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;\n")
			if disableLogBin {
				buf.WriteString("/*!32316 SET SQL_LOG_BIN=@OLD_SQL_LOG_BIN*/;\n")
			}
			buf.WriteString("/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;\n")
			if idempotent {
				buf.WriteString("/*!50700 SET @@SESSION.RBR_EXEC_MODE=STRICT*/;\n")
			}
			if autocommit {
				buf.WriteString("/*!50003 SET AUTOCOMMIT=@OLD_AUTOCOMMIT*/;\n")
			}
			if charset != "" {
				buf.WriteString("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n")
				buf.WriteString("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n")
				buf.WriteString("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n")
			}

			if p.Flashback {
				ioWriter.SetFooter(buf.Bytes())
			} else {
				ioWriter.Write(buf.Bytes())
			}
		case replication.TABLE_MAP_EVENT:
			r := e.Event.(*replication.TableMapEvent)
			buf := bytes.NewBuffer(nil)
			tableMapRawbytes[r.TableID] = e.RawData
			tableMapRawBase64[r.TableID] = b64.StdEncoding.EncodeToString(e.RawData)
			if !short {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(),
					e.Header.LogPos, r.Schema, r.Table, r.TableID) + "\n")
				ioWriter.Write(buf.Bytes())
			}
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1, replication.TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
			r := e.Event.(*replication.RowsEvent)
			buf := bytes.NewBuffer(nil)

			if len(e.RawData) <= replication.EventHeaderSize {
				//fmt.Println("xxxxx insert", "not matched")
				if !short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
						e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				}
			} else {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", BEGIN, Delimiter))

				buf.WriteString(rowsStart + "\n")
				buf.WriteString(tableMapRawBase64[r.TableID] + "\n")
				buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString(rowsEnd + "\n")
				if verbose {
					buf.Write(r.GetRowsEventPrinted())
				}
				buf.WriteString(fmt.Sprintf("%s%s\n", COMMIT, Delimiter))
			}
			ioWriter.Write(buf.Bytes())
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1, replication.TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
			buf := bytes.NewBuffer(nil)
			r := e.Event.(*replication.RowsEvent)

			if len(e.RawData) <= replication.EventHeaderSize {
				//fmt.Println("xxxxx delete", "not matched")
				if !short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
						e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				}
			} else {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", BEGIN, Delimiter))

				buf.WriteString(rowsStart + "\n")
				buf.WriteString(tableMapRawBase64[r.TableID] + "\n")
				buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString(rowsEnd + "\n")
				if verbose {
					buf.Write(r.GetRowsEventPrinted())
				}
				buf.WriteString(fmt.Sprintf("%s%s\n", COMMIT, Delimiter))
			}
			ioWriter.Write(buf.Bytes())
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			buf := bytes.NewBuffer(nil)
			r := e.Event.(*replication.RowsEvent)

			if len(e.RawData) <= replication.EventHeaderSize {
				//fmt.Println("xxxxx update", "not matched")
				if !short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
						e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				}
			} else {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.GetSchema(), r.Table.Table, r.TableID) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", BEGIN, Delimiter))

				buf.WriteString(rowsStart + "\n")
				buf.WriteString(tableMapRawBase64[r.TableID] + "\n")
				buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString(rowsEnd + "\n")
				if verbose {
					buf.Write(r.GetRowsEventPrinted())
				}
				buf.WriteString(fmt.Sprintf("%s%s\n", COMMIT, Delimiter))
			}
			ioWriter.Write(buf.Bytes())

		case replication.QUERY_EVENT:
			qe := e.Event.(*replication.QueryEvent)
			buf := bytes.NewBuffer(nil)
			if p.Flashback && qe.DbTableMatched {
				return errors.Errorf("statement error: %s", qe.Query)
			} else if p.TableFilter == nil || (p.TableFilter != nil && qe.DbTableMatched) {
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, qe.Schema) + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", BEGIN, Delimiter))

				if string(qe.Schema) != "" {
					buf.WriteString(fmt.Sprintf("USE `%s`%s\n", qe.Schema, Delimiter))
				}
				buf.WriteString(fmt.Sprintf("SET TIMESTAMP=%d%s\n", e.Header.Timestamp, Delimiter))
				buf.Write(qe.Query)
				buf.WriteString("\n" + Delimiter + "\n")
				buf.WriteString(fmt.Sprintf("%s%s\n", COMMIT, Delimiter))
			} else {
				if !short {
					buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
					buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d Db=%s",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, qe.Schema) + "\n")
				}
				// 不打印 statement
			}
			ioWriter.Write(buf.Bytes())
		//case replication.XID_EVENT:
		default:
			if !short {
				buf := bytes.NewBuffer(nil)
				buf.WriteString(fmt.Sprintf("# at %d\n", e.Header.LogPos-e.Header.EventSize))
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s EndLogPos=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos) + "\n")
				ioWriter.Write(buf.Bytes())
			}
		}
		//enc.Close()

		return nil
	}

	var tableFilter *db_table_filter.DbTableFilter
	var err error
	databases := viper.GetStringSlice("databases")
	tables := viper.GetStringSlice("tables")
	excludeDatabases := viper.GetStringSlice("exclude-databases")
	excludeTables := viper.GetStringSlice("exclude-tables")
	if len(databases)+len(tables)+len(excludeDatabases)+len(excludeTables) > 0 {
		if p.Flashback {
			excludeDatabases = append(excludeDatabases, "infodba_schema")
		}
		tableFilter, err = db_table_filter.NewDbTableFilter(databases, tables, excludeDatabases, excludeTables)
		if err != nil {
			return err
		}
		p.TableFilter = tableFilter
		if err = p.TableFilter.DbTableFilterCompile(); err != nil {
			return err
		}
	} else if p.Flashback {
		tableFilter, err = db_table_filter.NewDbTableFilter([]string{"*"}, []string{"*"}, []string{"infodba_schema"}, []string{"*"})
		p.TableFilter = tableFilter
		if err = p.TableFilter.DbTableFilterCompile(); err != nil {
			return err
		}
	}

	if rowsFilterExpr := viper.GetString("rows-filter"); rowsFilterExpr != "" {
		rowsFilter, err := replication.NewRowsFilter(rowsFilterExpr) // "col[0] == 2"
		if err != nil {
			return err
		}
		p.RowsFilter = rowsFilter
	}

	p.TimeFilter = timeFilter
	err = p.ParseFile(fileName, int64(timeFilter.StartPos), f)
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}
