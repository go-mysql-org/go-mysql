package main

import (
	"bytes"
	b64 "encoding/base64"
	"fmt"
	"io"
	"os"
	"time"

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
	BEGIN := []byte("BEGIN")
	COMMIT := []byte("COMMIT")
	Delimiter := "/*!*/;"

	//var tableMaps = map[uint64]*replication.TableMapEvent{}
	tableMapRawbytes := make(map[uint64][]byte)
	tableMapRawBase64 := make(map[uint64]string)

	p := replication.NewBinlogParser()
	p.Flashback = viper.GetBool("flashback")

	fileName := viper.GetString("file")
	startPos := viper.GetInt64("start-position")
	var startTs, stopTs uint32
	if start := viper.GetString("start-datetime"); start != "" {
		startDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("start-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse start-datetime")
		}
		startTs = uint32(startDatetime.Local().Unix())
	}
	if stop := viper.GetString("stop-datetime"); stop != "" {
		stopDatetime, err := time.ParseInLocation(time.DateTime, viper.GetString("stop-datetime"), time.Local)
		if err != nil {
			return errors.WithMessage(err, "parse stop-datetime")
		}
		stopTs = uint32(stopDatetime.Local().Unix())
	}
	verbose := viper.GetBool("verbose")
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
	rowsStart := "BINLOG '"
	rowsEnd := "'" + Delimiter

	f := func(e *replication.BinlogEvent) error {
		if e.Header.Timestamp < startTs || (stopTs > 0 && e.Header.Timestamp > stopTs) {
			return nil
		}
		//enc := b64.NewEncoder(b64.StdEncoding, ioWriter)
		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			r := e.Event.(*replication.FormatDescriptionEvent)
			buf := bytes.NewBuffer(nil)
			buf.WriteString("DELIMITER " + Delimiter + "\n")
			buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Version=%d",
				unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, r.Version) + "\n")
			buf.WriteString(rowsStart + "\n")
			buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
			buf.WriteString(rowsEnd + "\n")

			//fmt.Fprintf(ioWriter, b64RawString)
			ioWriter.SetHeader(buf.Bytes())

			//_, _ = enc.Write(e.RawData)
			//enc.Close()
			///fmt.Fprintf(ioWriter, b64.StdEncoding.EncodeToString(e.RawData))
			///fmt.Fprintf(ioWriter, rowsEnd)
			//r := e.Event.(*replication.FormatDescriptionEvent)
			//fmt.Printf("# glob_description_event: %+v\n", r.Version)
		case replication.ROTATE_EVENT:
			//fmt.Fprintf(ioWriter, "DELIMITER ;\n")
			ioWriter.SetFooter([]byte("\nDELIMITER ;\n"))
			//r := e.Event.(*replication.RotateEvent)
			//fmt.Printf("# End of log file, next: %s\n", r.NextLogName)
		case replication.TABLE_MAP_EVENT:
			r := e.Event.(*replication.TableMapEvent)
			tableMapRawbytes[r.TableID] = e.RawData
			tableMapRawBase64[r.TableID] = b64.StdEncoding.EncodeToString(e.RawData)
			// set table map cache
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1, replication.TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
			if len(e.RawData) <= replication.EventHeaderSize {
				//fmt.Println("xxxxx insert", "not matched")
			} else {
				r := e.Event.(*replication.RowsEvent)
				buf := bytes.NewBuffer(nil)

				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.Schema, r.Table.Table, r.TableID) + "\n")
				evType := fmt.Sprintf("header:%s rows:%s", e.Header.EventType.String(), r.GetEventType().String())
				buf.WriteString(evType + "\n")
				buf.WriteString(rowsStart + "\n")
				buf.WriteString(tableMapRawBase64[r.TableID] + "\n")
				buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString(rowsEnd + "\n")
				if verbose {
					buf.Write(r.GetRowsEventPrinted())
				}
				ioWriter.Write(buf.Bytes())

				/*
					fmt.Fprint(ioWriter, "BEGIN"+Delimiter+"\n")
					comment := fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s Table=%s TableID=%d",
						unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(), e.Header.LogPos, r.Table.Schema, r.Table.Table, r.TableID) + "\n"
					fmt.Fprint(ioWriter, comment)
					fmt.Fprint(ioWriter, rowsStart)
					fmt.Fprint(ioWriter, tableMapRawBase64[r.TableID]+"\n")
					_, _ = enc.Write(e.RawData)
					_ = enc.Close()
					fmt.Fprint(ioWriter, rowsEnd)
					r.PrintVerbose(ioWriter)
					fmt.Fprint(ioWriter, "COMMIT"+Delimiter+"\n")
				*/
			}

		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1, replication.TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
			if len(e.RawData) <= replication.EventHeaderSize {
				//fmt.Println("xxxxx delete", "not matched")
			} else {
				buf := bytes.NewBuffer(nil)

				r := e.Event.(*replication.RowsEvent)
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.Schema, r.Table.Table, r.TableID) + "\n")
				evType := fmt.Sprintf("header:%s rows:%s", e.Header.EventType.String(), r.GetEventType().String())
				buf.WriteString(evType + "\n")
				buf.WriteString(rowsStart + "\n")
				buf.WriteString(tableMapRawBase64[r.TableID] + "\n")
				buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString(rowsEnd + "\n")
				if verbose {
					buf.Write(r.GetRowsEventPrinted())
				}
				ioWriter.Write(buf.Bytes())
			}

			///fmt.Fprintf(ioWriter, rowsStart)
			///fmt.Fprintf(ioWriter, tableMapRawBase64[r.TableID]+"\n")
			///fmt.Fprintf(ioWriter, b64.StdEncoding.EncodeToString(e.RawData))
			///fmt.Fprintf(ioWriter, rowsEnd)
			//_, _ = enc.Write(tableMapRawbytes[r.TableID])
			//enc.Close()
			//fmt.Fprintf(ioWriter, "\n")
			//_, _ = enc.Write(e.RawData)
			//enc.Close()

			//fmt.Printf("# event type: %s\n", e.Header.EventType.String())
			//fmt.Printf("# delete rows: %+v\n", r.Rows)
			//r.Dump(os.Stdout)
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			if len(e.RawData) <= replication.EventHeaderSize {
				//fmt.Println("xxxxx update", "not matched")
			} else {
				buf := bytes.NewBuffer(nil)

				r := e.Event.(*replication.RowsEvent)
				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(),
					e.Header.LogPos, r.Table.Schema, r.Table.Table, r.TableID) + "\n")
				buf.WriteString(rowsStart + "\n")
				buf.WriteString(tableMapRawBase64[r.TableID] + "\n")
				buf.WriteString(b64.StdEncoding.EncodeToString(e.RawData) + "\n")
				buf.WriteString(rowsEnd + "\n")
				if verbose {
					buf.Write(r.GetRowsEventPrinted())
				}
				ioWriter.Write(buf.Bytes())

				//r.PrintVerbose(os.Stdout)
			}

			///fmt.Fprintf(ioWriter, rowsStart)
			///fmt.Fprintf(ioWriter, tableMapRawBase64[r.TableID]+"\n")
			///fmt.Fprintf(ioWriter, b64.StdEncoding.EncodeToString(e.RawData))
			///fmt.Fprintf(ioWriter, rowsEnd)
			//_, _ = enc.Write(tableMapRawbytes[r.TableID])
			//enc.Close()
			//fmt.Fprintf(ioWriter, "\n")
			//_, _ = enc.Write(e.RawData)
			//enc.Close()

			//fmt.Printf("# event type: %s\n", e.Header.EventType.String())
			//fmt.Printf("# update rows, table_name=%s.%s: %+v\n", r.Table.Schema, r.Table.Table, r.Rows)
			//r.Dump(os.Stdout)
		case replication.QUERY_EVENT:
			qe := e.Event.(*replication.QueryEvent)
			if bytes.Equal(qe.Query, BEGIN) || bytes.Equal(qe.Query, COMMIT) {
				//fmt.Fprintf(iowriter, "%s%s\n", r.Query, Delimiter)
			}
			if p.Flashback && qe.DbTableMatched {
				return errors.Errorf("statement error")
			} else if p.TableFilter == nil || (p.TableFilter != nil && qe.DbTableMatched) {
				buf := bytes.NewBuffer(nil)

				buf.WriteString(fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, qe.Schema) + "\n")
				if string(qe.Schema) != "" {
					buf.WriteString(fmt.Sprintf("USE `%s`%s\n", qe.Schema, Delimiter))
				}
				buf.WriteString(fmt.Sprintf("SET TIMESTAMP=%d%s\n", e.Header.Timestamp, Delimiter))
				buf.Write(qe.Query)
				buf.WriteString("\n + Delimiter + \n")
				//fmt.Fprintf(ioWriter, queryString)
				ioWriter.Write(buf.Bytes())
				//fmt.Printf("\nquery event: %s\n", qe.Query)
			} else {
				// 不打印 statement
			}
		default:
			//fmt.Printf("# event type: %s\n", e.Header.EventType.String())
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

	err = p.ParseFile(fileName, startPos, f)
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}
