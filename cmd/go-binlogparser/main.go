package main

import (
	"bytes"
	b64 "encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
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

type BytesCache struct {
	filePrefix      string
	maxCacheSize    int
	cacheHeader     []string
	cacheFooter     []string
	cache           [][]byte
	currentIndex    int
	currentWritten  int
	currentFileName string

	currentIOWriter io.WriteCloser
	filePartId      int
}

func (c *BytesCache) Write(p []byte) (n int, err error) {
	if c.currentIOWriter == nil {
		return 0, errors.New("unknown writer")
	}
	pc := make([]byte, len(p))
	copy(pc, p)
	c.cache = append(c.cache, pc)
	//c.cache[c.currentIndex] = p
	c.currentIndex += 1
	c.currentWritten += len(p)
	if c.currentWritten >= c.maxCacheSize {
		c.currentWritten = 0
		c.currentIndex = 0
		for i := len(c.cache); i > 0; i-- {
			if i == len(c.cache) {
				c.currentIOWriter.Write([]byte(strings.Join(c.cacheHeader, "\n")))
			}
			_, err = c.currentIOWriter.Write(c.cache[i-1])
			if err != nil {
				return 0, err
			}
		}
		c.next()
		//return len(p), errors.New("cache full")
	}
	return len(p), nil
}

// NewBytesCache filePrefix may contain db_table name
func (c *BytesCache) next() {
	c.currentIOWriter.Close()
	c.cache = nil
	c.filePartId += 1
	c.currentFileName = fmt.Sprintf("%s.%03d.sql", c.filePrefix, c.filePartId)
	//c.currentIOWriter, _ = os.OpenFile(c.currentFileName, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	c.currentIOWriter, _ = os.Create(c.currentFileName)
}

func (c *BytesCache) SetHeader(s []string) {
	c.cacheHeader = s
}
func (c *BytesCache) SetFooter(s []string) {
	c.cacheFooter = s
}

func (c *BytesCache) Close() (err error) {
	for i := len(c.cache); i > 0; i-- {
		if i == len(c.cache) {
			c.currentIOWriter.Write([]byte(strings.Join(c.cacheHeader, "\n")))
		}
		_, err = c.currentIOWriter.Write(c.cache[i-1])
		if err != nil {
			return err
		}
	}
	c.currentIOWriter.Write([]byte(strings.Join(c.cacheFooter, "\n")))
	c.cache = nil
	return c.currentIOWriter.Close()
}

func NewBytesCache(filePrefix string, cacheSize int) *BytesCache {
	fmt.Println("xxx", filePrefix)
	bc := &BytesCache{
		filePrefix:   filePrefix,
		maxCacheSize: cacheSize,
	}
	bc.currentFileName = fmt.Sprintf("%s.%03d.sql", filePrefix, bc.filePartId)
	//bc.currentIOWriter, _ = os.OpenFile(bc.currentFileName, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	bc.currentIOWriter, _ = os.Create(bc.currentFileName)
	return bc
}

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

	//printParser := replication.NewBinlogParser()
	//fileCache, _ := os.OpenFile("./test.txt", os.O_APPEND|os.O_WRONLY, os.ModePerm)
	//defer fileCache.Close()
	//cacheWriter := bufio.NewWriterSize(fileCache, 128*1024*1024*1024)

	ioWriter := NewBytesCache(fileName, 128*1024*1024*1024)
	defer ioWriter.Close()

	f := func(e *replication.BinlogEvent) error {
		if e.Header.Timestamp < startTs || (stopTs > 0 && e.Header.Timestamp > stopTs) {
			return nil
		}

		//enc := b64.NewEncoder(b64.StdEncoding, ioWriter)

		//b64.StdEncoding.EncodeToString()
		rowsStart := "BINLOG '\n"
		rowsEnd := "'" + Delimiter + "\n"
		//_, _ = enc.Write(e.RawData)
		//ii := bytes.NewReader(e.RawData)
		//printParser.ParseSingleEvent(ii,)

		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			cacheHeader := []string{}
			cacheHeader = append(cacheHeader, fmt.Sprintf("DELIMITER %s\n", Delimiter))
			///fmt.Fprintf(ioWriter, "DELIMITER %s\n", Delimiter)
			///fmt.Fprintf(ioWriter, rowsStart)
			r := e.Event.(*replication.FormatDescriptionEvent)
			b64RawString := ""
			b64RawString += fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Version=%d",
				unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, r.Version) + "\n"
			b64RawString += rowsStart
			b64RawString += b64.StdEncoding.EncodeToString(e.RawData) + "\n"
			b64RawString += rowsEnd
			cacheHeader = append(cacheHeader, b64RawString)
			//fmt.Fprintf(ioWriter, b64RawString)
			ioWriter.SetHeader(cacheHeader)

			//_, _ = enc.Write(e.RawData)
			//enc.Close()
			///fmt.Fprintf(ioWriter, b64.StdEncoding.EncodeToString(e.RawData))
			///fmt.Fprintf(ioWriter, rowsEnd)
			//r := e.Event.(*replication.FormatDescriptionEvent)
			//fmt.Printf("# glob_description_event: %+v\n", r.Version)
		case replication.ROTATE_EVENT:
			//fmt.Fprintf(ioWriter, "DELIMITER ;\n")
			ioWriter.SetFooter([]string{"", "DELIMITER ;\n"})
			//r := e.Event.(*replication.RotateEvent)
			//fmt.Printf("# End of log file, next: %s\n", r.NextLogName)
		case replication.TABLE_MAP_EVENT:
			r := e.Event.(*replication.TableMapEvent)
			tableMapRawbytes[r.TableID] = e.RawData
			tableMapRawBase64[r.TableID] = b64.StdEncoding.EncodeToString(e.RawData)
			// set table map cache
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.TENDB_WRITE_ROWS_COMPRESSED_EVENT_V1, replication.TENDB_WRITE_ROWS_COMPRESSED_EVENT_V2:
			if len(e.RawData) <= replication.EventHeaderSize {
				fmt.Println("xxxxx insert", "not matched")
			} else {
				r := e.Event.(*replication.RowsEvent)
				b64RawString := ""
				b64RawString += fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(), e.Header.LogPos, r.Table.Schema, r.Table.Table, r.TableID) + "\n"
				b64RawString += rowsStart
				b64RawString += tableMapRawBase64[r.TableID] + "\n"
				b64RawString += b64.StdEncoding.EncodeToString(e.RawData) + "\n"
				b64RawString += rowsEnd
				fmt.Fprintf(ioWriter, b64RawString)

				r.PrintVerbose(os.Stdout)
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
			//fmt.Printf("# insert rows: %+v\n", r.Rows)
			////e.SetRawDataIndex(replication.EventTypePos, byte(replication.DELETE_ROWS_EVENTv1))
			//e.Dump(os.Stdout)
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.TENDB_DELETE_ROWS_COMPRESSED_EVENT_V1, replication.TENDB_DELETE_ROWS_COMPRESSED_EVENT_V2:
			if len(e.RawData) <= replication.EventHeaderSize {
				fmt.Println("xxxxx delete", "not matched")
			} else {
				r := e.Event.(*replication.RowsEvent)
				b64RawString := ""
				b64RawString += fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(), e.Header.LogPos, r.Table.Schema, r.Table.Table, r.TableID) + "\n"
				b64RawString += rowsStart
				b64RawString += tableMapRawBase64[r.TableID] + "\n"
				b64RawString += b64.StdEncoding.EncodeToString(e.RawData) + "\n"
				b64RawString += rowsEnd
				fmt.Fprintf(ioWriter, b64RawString)
				r.PrintVerbose(os.Stdout)
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
				fmt.Println("xxxxx update", "not matched")
			} else {
				r := e.Event.(*replication.RowsEvent)
				b64RawString := ""
				b64RawString += fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s Table=%s TableID=%d",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, r.GetEventType().String(), e.Header.LogPos, r.Table.Schema, r.Table.Table, r.TableID) + "\n"
				b64RawString += rowsStart
				b64RawString += tableMapRawBase64[r.TableID] + "\n"
				b64RawString += b64.StdEncoding.EncodeToString(e.RawData) + "\n"
				b64RawString += rowsEnd
				fmt.Fprintf(ioWriter, b64RawString)
				r.PrintVerbose(os.Stdout)
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
				queryString := ""
				queryString += fmt.Sprintf("# Timestamp=%s ServerId=%d EventType=%s LogPos=%d Db=%s",
					unixTimeToStr(e.Header.Timestamp), e.Header.ServerID, e.Header.EventType.String(), e.Header.LogPos, qe.Schema) + "\n"
				if string(qe.Schema) != "" {
					queryString += fmt.Sprintf("USE `%s`%s\n", qe.Schema, Delimiter)
				}
				queryString += fmt.Sprintf("SET TIMESTAMP=%d%s\n", e.Header.Timestamp, Delimiter)
				queryString += string(qe.Query) + "\n" + Delimiter + "\n"
				fmt.Fprintf(ioWriter, queryString)
				fmt.Printf("\nquery event: %s\n", qe.Query)
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
	}

	if rowsFilterExpr := viper.GetString("rows-filter"); rowsFilterExpr != "" {
		rowsFilter, err := replication.NewRowsFilter(rowsFilterExpr) // "col[0] == 2"
		if err != nil {
			return err
		}
		p.RowsFilter = rowsFilter
	}
	p.Flashback = viper.GetBool("flashback")

	err = p.ParseFile(fileName, startPos, f)
	if err != nil {
		println(err.Error())
	}
	return nil
}
