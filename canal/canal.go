package canal

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/dump"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	driverMysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/siddontang/go-log/log"
)

// Canal can sync your MySQL data into everywhere, like Elasticsearch, Redis, etc...
// MySQL must open row format for binlog
type Canal struct {
	m sync.Mutex

	cfg *Config

	parser     *parser.Parser
	master     *masterInfo
	dumper     *dump.Dumper
	dumped     bool
	dumpDoneCh chan struct{}
	syncer     *replication.BinlogSyncer

	eventHandler EventHandler

	connLock sync.Mutex
	conn     *client.Conn

	tableLock          sync.RWMutex
	tables             map[string]*schema.Table
	errorTablesGetTime map[string]time.Time

	tableMatchCache   map[string]bool
	includeTableRegex []*regexp.Regexp
	excludeTableRegex []*regexp.Regexp

	delay *uint32

	ctx    context.Context
	cancel context.CancelFunc

	lastEventSentTime time.Time
	heartbeatInterval time.Duration
}

// canal will retry fetching unknown table's meta after UnknownTableRetryPeriod
var UnknownTableRetryPeriod = time.Second * time.Duration(10)
var ErrExcludedTable = errors.New("excluded table meta")

func NewCanal(cfg *Config) (*Canal, error) {
	c := new(Canal)
	c.cfg = cfg

	c.ctx, c.cancel = context.WithCancel(context.Background())

	if cfg.WaitTimeBetweenConnectionSeconds <= 0 {
		cfg.WaitTimeBetweenConnectionSeconds = time.Duration(5) * time.Second
	}

	c.dumpDoneCh = make(chan struct{})
	c.eventHandler = &DummyEventHandler{}
	c.parser = parser.New()
	c.tables = make(map[string]*schema.Table)
	if c.cfg.DiscardNoMetaRowEvent {
		c.errorTablesGetTime = make(map[string]time.Time)
	}
	c.master = &masterInfo{}

	c.delay = new(uint32)

	var err error

	if err = c.prepareDumper(); err != nil {
		return nil, errors.Trace(err)
	}

	if err := c.GetColumnsCharsets(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = c.prepareSyncer(); err != nil {
		return nil, errors.Trace(err)
	}

	if err := c.checkBinlogRowFormat(); err != nil {
		return nil, errors.Trace(err)
	}

	// init table filter
	if n := len(c.cfg.IncludeTableRegex); n > 0 {
		c.includeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.IncludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			c.includeTableRegex[i] = reg
		}
	}

	if n := len(c.cfg.ExcludeTableRegex); n > 0 {
		c.excludeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.ExcludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			c.excludeTableRegex[i] = reg
		}
	}

	if c.includeTableRegex != nil || c.excludeTableRegex != nil {
		c.tableMatchCache = make(map[string]bool)
	}

	if c.cfg.HeartbeatIntervalSeconds > 0 {
		c.heartbeatInterval = time.Duration(c.cfg.HeartbeatIntervalSeconds) * time.Second
		c.lastEventSentTime = time.Now()
		log.Infof("Heartbeat interval set to %d seconds", c.cfg.HeartbeatIntervalSeconds)
	}

	return c, nil
}

func (c *Canal) prepareDumper() error {
	var err error
	dumpPath := c.cfg.Dump.ExecutionPath
	if len(dumpPath) == 0 {
		// ignore mysqldump, use binlog only
		return nil
	}

	if c.dumper, err = dump.NewDumper(dumpPath,
		c.cfg.Addr, c.cfg.User, c.cfg.Password); err != nil {
		return errors.Trace(err)
	}

	if c.dumper == nil {
		//no mysqldump, use binlog only
		return nil
	}

	dbs := c.cfg.Dump.Databases
	tables := c.cfg.Dump.Tables
	tableDB := c.cfg.Dump.TableDB

	if len(tables) == 0 {
		c.dumper.AddDatabases(dbs...)
	} else {
		c.dumper.AddTables(tableDB, tables...)
	}

	charset := c.cfg.Charset
	c.dumper.SetCharset(charset)

	c.dumper.SetWhere(c.cfg.Dump.Where)
	c.dumper.SkipMasterData(c.cfg.Dump.SkipMasterData)
	c.dumper.SetMaxAllowedPacket(c.cfg.Dump.MaxAllowedPacketMB)
	c.dumper.SetProtocol(c.cfg.Dump.Protocol)
	c.dumper.SetExtraOptions(c.cfg.Dump.ExtraOptions)
	// Use hex blob for mysqldump
	c.dumper.SetHexBlob(true)

	for _, ignoreTable := range c.cfg.Dump.IgnoreTables {
		if seps := strings.Split(ignoreTable, ","); len(seps) == 2 {
			c.dumper.AddIgnoreTables(seps[0], seps[1])
		}
	}

	if c.cfg.Dump.DiscardErr {
		c.dumper.SetErrOut(ioutil.Discard)
	} else {
		c.dumper.SetErrOut(os.Stderr)
	}

	return nil
}

func (c *Canal) GetDelay() uint32 {
	return atomic.LoadUint32(c.delay)
}

// Run will first try to dump all data from MySQL master `mysqldump`,
// then sync from the binlog position in the dump data.
// It will run forever until meeting an error or Canal closed.
func (c *Canal) Run() error {
	return c.run()
}

// RunFrom will sync from the binlog position directly, ignore mysqldump.
func (c *Canal) RunFrom(pos mysql.Position) error {
	c.master.Update(pos)

	return c.Run()
}

// Start from selected GTIDSet
func (c *Canal) StartFromGTID(set mysql.GTIDSet) error {
	c.master.UpdateGTIDSet(set)

	return c.Run()
}

// Dump all data from MySQL master `mysqldump`, ignore sync binlog.
func (c *Canal) Dump() error {
	if c.dumped {
		return errors.New("the method Dump can't be called twice")
	}
	c.dumped = true
	defer close(c.dumpDoneCh)
	return c.dump()
}

func (c *Canal) run() error {
	defer func() {
		c.cancel()
	}()

	c.master.UpdateTimestamp(uint32(time.Now().Unix()))

	if !c.dumped {
		c.dumped = true

		err := c.tryDump()
		close(c.dumpDoneCh)

		if err != nil {
			log.Errorf("canal dump mysql err: %v", err)
			return errors.Trace(err)
		}
	}

	if err := c.runSyncBinlog(); err != nil {
		if errors.Cause(err) != context.Canceled {
			log.Errorf("canal start sync binlog err: %v", err)
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *Canal) Close() {
	log.Debugf("closing canal")
	c.m.Lock()
	defer c.m.Unlock()

	c.cancel()
	c.syncer.Close()
	c.connLock.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.connLock.Unlock()

	_ = c.eventHandler.OnPosSynced(c.master.Position(), c.master.GTIDSet(), true)
}

func (c *Canal) WaitDumpDone() <-chan struct{} {
	return c.dumpDoneCh
}

func (c *Canal) Ctx() context.Context {
	return c.ctx
}

func (c *Canal) checkTableMatch(key string) bool {
	// no filter, return true
	if c.tableMatchCache == nil {
		return true
	}

	c.tableLock.RLock()
	rst, ok := c.tableMatchCache[key]
	c.tableLock.RUnlock()
	if ok {
		// cache hit
		return rst
	}
	matchFlag := false
	// check include
	if c.includeTableRegex != nil {
		for _, reg := range c.includeTableRegex {
			if reg.MatchString(key) {
				matchFlag = true
				break
			}
		}
	}
	// check exclude
	if matchFlag && c.excludeTableRegex != nil {
		for _, reg := range c.excludeTableRegex {
			if reg.MatchString(key) {
				matchFlag = false
				break
			}
		}
	}
	c.tableLock.Lock()
	c.tableMatchCache[key] = matchFlag
	c.tableLock.Unlock()
	return matchFlag
}

func (c *Canal) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	// if table is excluded, return error and skip parsing event or dump
	if !c.checkTableMatch(key) {
		return nil, ErrExcludedTable
	}
	c.tableLock.RLock()
	t, ok := c.tables[key]
	c.tableLock.RUnlock()

	if ok {
		return t, nil
	}

	if c.cfg.DiscardNoMetaRowEvent {
		c.tableLock.RLock()
		lastTime, ok := c.errorTablesGetTime[key]
		c.tableLock.RUnlock()
		if ok && time.Now().Sub(lastTime) < UnknownTableRetryPeriod {
			return nil, schema.ErrMissingTableMeta
		}
	}

	t, err := schema.NewTable(c, db, table)
	if err != nil {
		// check table not exists
		if ok, err1 := schema.IsTableExist(c, db, table); err1 == nil && !ok {
			return nil, schema.ErrTableNotExist
		}
		// work around : RDS HAHeartBeat
		// ref : https://github.com/alibaba/canal/blob/master/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L385
		// issue : https://github.com/alibaba/canal/issues/222
		// This is a common error in RDS that canal can't get HAHealthCheckSchema's meta, so we mock a table meta.
		// If canal just skip and log error, as RDS HA heartbeat interval is very short, so too many HAHeartBeat errors will be logged.
		if key == schema.HAHealthCheckSchema {
			// mock ha_health_check meta
			ta := &schema.Table{
				Schema:  db,
				Name:    table,
				Columns: make([]schema.TableColumn, 0, 2),
				Indexes: make([]*schema.Index, 0),
			}
			ta.AddColumn("id", "bigint(20)", "", "")
			ta.AddColumn("type", "char(1)", "", "")
			c.tableLock.Lock()
			c.tables[key] = ta
			c.tableLock.Unlock()
			return ta, nil
		}
		// if DiscardNoMetaRowEvent is true, we just log this error
		if c.cfg.DiscardNoMetaRowEvent {
			c.tableLock.Lock()
			c.errorTablesGetTime[key] = time.Now()
			c.tableLock.Unlock()
			// log error and return ErrMissingTableMeta
			log.Errorf("canal get table meta err: %v", errors.Trace(err))
			return nil, schema.ErrMissingTableMeta
		}
		return nil, err
	}

	c.tableLock.Lock()
	c.tables[key] = t
	if c.cfg.DiscardNoMetaRowEvent {
		// if get table info success, delete this key from errorTablesGetTime
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()

	return t, nil
}

// ClearTableCache clear table cache
func (c *Canal) ClearTableCache(db []byte, table []byte) {
	key := fmt.Sprintf("%s.%s", db, table)
	c.tableLock.Lock()
	delete(c.tables, key)
	if c.cfg.DiscardNoMetaRowEvent {
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()
}

// CheckBinlogRowImage checks MySQL binlog row image, must be in FULL, MINIMAL, NOBLOB
func (c *Canal) CheckBinlogRowImage(image string) error {
	// need to check MySQL binlog row image? full, minimal or noblob?
	// now only log
	if c.cfg.Flavor == mysql.MySQLFlavor {
		if res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'`); err != nil {
			return errors.Trace(err)
		} else {
			// MySQL has binlog row image from 5.6, so older will return empty
			rowImage, _ := res.GetString(0, 1)
			if rowImage != "" && !strings.EqualFold(rowImage, image) {
				return errors.Errorf("MySQL uses %s binlog row image, but we want %s", rowImage, image)
			}
		}
	}

	return nil
}

func (c *Canal) checkBinlogRowFormat() error {
	res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE 'binlog_format';`)
	if err != nil {
		return errors.Trace(err)
	} else if f, _ := res.GetString(0, 1); f != "ROW" {
		return errors.Errorf("binlog must ROW format, but %s now", f)
	}

	return nil
}

func isSafeIdentifier(s string) bool {
	for _, r := range s {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-') {
			return false
		}
	}
	return len(s) > 0
}

func (c *Canal) GenerateCharsetQuery() (string, error) {
	query := `
       SELECT 
          c.ORDINAL_POSITION,
          COALESCE(
             CASE 
                WHEN c.CHARACTER_SET_NAME IS NOT NULL THEN c.CHARACTER_SET_NAME
                WHEN c.DATA_TYPE IN ('binary','varbinary','tinyblob','blob','mediumblob','longblob') THEN col.CHARACTER_SET_NAME
                ELSE col.CHARACTER_SET_NAME
             END,
             'utf8mb4'
          ) AS CHARACTER_SET_NAME,
          c.COLUMN_NAME
       FROM 
          information_schema.COLUMNS c
       LEFT JOIN information_schema.TABLES t
          ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
       LEFT JOIN information_schema.COLLATIONS col
          ON col.COLLATION_NAME = t.TABLE_COLLATION
       WHERE 
          c.TABLE_SCHEMA = ?
          AND c.TABLE_NAME = ?
          AND (c.CHARACTER_SET_NAME IS NOT NULL 
               OR c.DATA_TYPE IN ('binary','varbinary','tinyblob','blob','mediumblob','longblob')
               OR c.DATA_TYPE IN ('varchar','char','text','tinytext','mediumtext','longtext'));
    `

	return query, nil
}

func (c *Canal) setColumnsCharsetFromRows(tableRegex string, rows *sql.Rows) error {
	c.cfg.ColumnCharset[tableRegex] = make(map[int]string)
	for rows.Next() {
		var ordinal int
		var charset, columnName sql.NullString
		if err := rows.Scan(&ordinal, &charset, &columnName); err != nil {
			return errors.Annotate(err, "failed to scan charset row")
		}

		// Handle NULL charset values properly
		charsetValue := "utf8mb4" // default charset
		if charset.Valid && charset.String != "" {
			charsetValue = charset.String
		}

		c.cfg.ColumnCharset[tableRegex][ordinal] = charsetValue
		log.Infof("Column Name: %s, Ordinal: %d, Charset: %s", columnName.String, ordinal, charsetValue)
	}

	return rows.Err()
}

func (c *Canal) GetColumnsCharsets() error {
	c.cfg.ColumnCharset = make(map[string]map[int]string)

	var dsn string
	if c.cfg.TLSConfig != nil {
		if err := driverMysql.RegisterTLSConfig("custom", c.cfg.TLSConfig); err != nil {
			return fmt.Errorf("failed to register TLS config: %w", err)
		}
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/information_schema?tls=custom", c.cfg.User, c.cfg.Password, c.cfg.Addr)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/information_schema", c.cfg.User, c.cfg.Password, c.cfg.Addr)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open DB connection: %w", err)
	}
	defer db.Close()

	for _, tableRegex := range c.cfg.IncludeTableRegex {
		parts := strings.Split(tableRegex, ".")
		if len(parts) != 2 {
			return fmt.Errorf("invalid tableRegex format, expected db.table")
		}
		dbName, tableName := parts[0], parts[1]

		if !isSafeIdentifier(dbName) || !isSafeIdentifier(tableName) {
			return fmt.Errorf("invalid characters in db or table name: %s.%s", dbName, tableName)
		}

		query, err := c.GenerateCharsetQuery()
		if err != nil {
			return fmt.Errorf("failed to generate charset query: %w", err)
		}
		rows, err := db.QueryContext(c.ctx, query, dbName, tableName)
		if err != nil {
			return fmt.Errorf("error occurred while executing query: %s on db: %s on table: %s. error: %v",
				query, dbName, tableName, errors.Trace(err))
		}

		// Process rows with proper error handling
		defer rows.Close()
		if err := c.setColumnsCharsetFromRows(tableRegex, rows); err != nil {
			return fmt.Errorf("failed to set charset from rows for table %s: %w", tableRegex, err)
		}
	}

	return nil
}

func (c *Canal) prepareSyncer() error {
	cfg := replication.BinlogSyncerConfig{
		ServerID:                         c.cfg.ServerID,
		Flavor:                           c.cfg.Flavor,
		User:                             c.cfg.User,
		Password:                         c.cfg.Password,
		Charset:                          c.cfg.Charset,
		ColumnCharset:                    c.cfg.ColumnCharset,
		HeartbeatPeriod:                  c.cfg.HeartbeatPeriod,
		ReadTimeout:                      c.cfg.ReadTimeout,
		UseDecimal:                       c.cfg.UseDecimal,
		ParseTime:                        c.cfg.ParseTime,
		SemiSyncEnabled:                  c.cfg.SemiSyncEnabled,
		MaxReconnectAttempts:             c.cfg.MaxReconnectAttempts,
		DisableRetrySync:                 c.cfg.DisableRetrySync,
		TimestampStringLocation:          c.cfg.TimestampStringLocation,
		TLSConfig:                        c.cfg.TLSConfig,
		WaitTimeBetweenConnectionSeconds: c.cfg.WaitTimeBetweenConnectionSeconds,
	}

	if strings.Contains(c.cfg.Addr, "/") {
		cfg.Host = c.cfg.Addr
	} else {
		seps := strings.Split(c.cfg.Addr, ":")
		if len(seps) != 2 {
			return errors.Errorf("invalid mysql addr format %s, must host:port", c.cfg.Addr)
		}

		port, err := strconv.ParseUint(seps[1], 10, 16)
		if err != nil {
			return errors.Trace(err)
		}

		cfg.Host = seps[0]
		cfg.Port = uint16(port)
	}

	c.syncer = replication.NewBinlogSyncer(cfg)

	return nil
}

// Execute a SQL
func (c *Canal) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	argF := make([]func(*client.Conn), 0)
	if c.cfg.TLSConfig != nil {
		argF = append(argF, func(conn *client.Conn) {
			conn.SetTLSConfig(c.cfg.TLSConfig)
		})
	}
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if c.conn == nil {
			c.conn, err = client.Connect(c.cfg.Addr, c.cfg.User, c.cfg.Password, "", argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = c.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			c.conn.Close()
			c.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (c *Canal) SyncedPosition() mysql.Position {
	return c.master.Position()
}

func (c *Canal) SyncedTimestamp() uint32 {
	return c.master.timestamp
}

func (c *Canal) SyncedGTIDSet() mysql.GTIDSet {
	return c.master.GTIDSet()
}

// shouldSendHeartbeat checks if enough time has passed since the last event was sent
func (c *Canal) shouldSendHeartbeat() bool {
	if c.heartbeatInterval == 0 {
		return false
	}
	return time.Since(c.lastEventSentTime) >= c.heartbeatInterval
}

// sendAsHeartbeat converts a BinlogEvent to a heartbeat event and sends it to the handler
func (c *Canal) sendAsHeartbeat(e *replication.BinlogEvent) {
	_, ok := e.Event.(*replication.RowsEvent)
	if !ok {
		log.Warnf("Failed to send heartbeat: event is not a RowsEvent, type: %T", e.Event)
		return
	}

	heartbeat := &RowsEvent{
		Table:  nil,
		Action: "heartbeat",
		Rows:   nil,
		Header: e.Header,
	}
	heartbeat.Header.Gtid = c.SyncedGTIDSet()
	err := c.eventHandler.OnRow(heartbeat)
	if err != nil {
		posInfo := formatPositionInfo(e.Header.FileName, e.Header.LogPos, heartbeat.Header.Gtid)
		log.Warnf("Failed to send heartbeat at %s: %v", posInfo, err)
	}
}

// Helper function to format position info with optional GTID
func formatPositionInfo(fileName string, logPos uint32, gtid mysql.GTIDSet) string {
	if gtid != nil && gtid.String() != "" {
		return fmt.Sprintf("position (%s, %d), GTID: %s", fileName, logPos, gtid.String())
	}
	return fmt.Sprintf("position (%s, %d)", fileName, logPos)
}
