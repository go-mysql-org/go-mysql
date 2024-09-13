package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/siddontang/go-log/log"
)

type RowEventPrinter struct {
	m sync.Mutex

	cfg *canal.Config

	parser       *parser.Parser
	eventHandler canal.EventHandler

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
}

type FlashbackEventHandler struct {
	canal.DummyEventHandler
}

func (h *FlashbackEventHandler) OnRow(e *canal.RowsEvent) error {
	fmt.Printf("%v\n", e)

	return nil
}

func NewRowEventPrinter(cfg *canal.Config) (*RowEventPrinter, error) {
	c := new(RowEventPrinter)
	if cfg.Logger == nil {
		streamHandler, _ := log.NewStreamHandler(os.Stdout)
		cfg.Logger = log.NewDefault(streamHandler)
	}
	if cfg.Dialer == nil {
		dialer := &net.Dialer{}
		cfg.Dialer = dialer.DialContext
	}
	c.cfg = cfg

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.eventHandler = &FlashbackEventHandler{}
	c.parser = parser.New()
	c.tables = make(map[string]*schema.Table)
	if c.cfg.DiscardNoMetaRowEvent {
		c.errorTablesGetTime = make(map[string]time.Time)
	}

	c.delay = new(uint32)

	//var err error

	if err := c.checkBinlogRowFormat(); err != nil {
		return nil, errors.Trace(err)
	}

	if err := c.initTableFilter(); err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

func (c *RowEventPrinter) initTableFilter() error {
	if n := len(c.cfg.IncludeTableRegex); n > 0 {
		c.includeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.IncludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return errors.Trace(err)
			}
			c.includeTableRegex[i] = reg
		}
	}

	if n := len(c.cfg.ExcludeTableRegex); n > 0 {
		c.excludeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.ExcludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return errors.Trace(err)
			}
			c.excludeTableRegex[i] = reg
		}
	}

	if c.includeTableRegex != nil || c.excludeTableRegex != nil {
		c.tableMatchCache = make(map[string]bool)
	}
	return nil
}

func (c *RowEventPrinter) checkTableMatch(key string) bool {
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
	} else {
		matchFlag = true
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

func (c *RowEventPrinter) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	// if table is excluded, return error and skip parsing event or dump
	if !c.checkTableMatch(key) {
		return nil, canal.ErrExcludedTable
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
		if ok && time.Since(lastTime) < canal.UnknownTableRetryPeriod {
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
			c.cfg.Logger.Errorf("canal get table meta err: %v", errors.Trace(err))
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
func (c *RowEventPrinter) ClearTableCache(db []byte, table []byte) {
	key := fmt.Sprintf("%s.%s", db, table)
	c.tableLock.Lock()
	delete(c.tables, key)
	if c.cfg.DiscardNoMetaRowEvent {
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()
}

// SetTableCache sets table cache value for the given table
func (c *RowEventPrinter) SetTableCache(db []byte, table []byte, schema *schema.Table) {
	key := fmt.Sprintf("%s.%s", db, table)
	c.tableLock.Lock()
	c.tables[key] = schema
	if c.cfg.DiscardNoMetaRowEvent {
		// if get table info success, delete this key from errorTablesGetTime
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()
}

// CheckBinlogRowImage checks MySQL binlog row image, must be in FULL, MINIMAL, NOBLOB
func (c *RowEventPrinter) CheckBinlogRowImage(image string) error {
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

func (c *RowEventPrinter) checkBinlogRowFormat() error {
	res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE 'binlog_format';`)
	if err != nil {
		return errors.Trace(err)
	} else if f, _ := res.GetString(0, 1); f != "ROW" {
		return errors.Errorf("binlog must ROW format, but %s now", f)
	}

	return nil
}

// Execute a SQL
func (c *RowEventPrinter) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	argF := make([]client.Option, 0)
	if c.cfg.TLSConfig != nil {
		argF = append(argF, func(conn *client.Conn) error {
			conn.SetTLSConfig(c.cfg.TLSConfig)
			return nil
		})
	}

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if c.conn == nil {
			c.conn, err = c.connect(argF...)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = c.conn.Execute(cmd, args...)
		if err != nil {
			if mysql.ErrorEqual(err, mysql.ErrBadConn) {
				c.conn.Close()
				c.conn = nil
				continue
			}
			return nil, err
		}
		break
	}
	return rr, err
}

func (c *RowEventPrinter) connect(options ...client.Option) (*client.Conn, error) {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*10)
	defer cancel()

	return client.ConnectWithDialer(ctx, "", c.cfg.Addr,
		c.cfg.User, c.cfg.Password, "", c.cfg.Dialer, options...)
}
