package canal

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/dump"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/siddontang/go/log"
	"github.com/siddontang/go/sync2"
)

var errCanalClosed = errors.New("canal was closed")

// Canal can sync your MySQL data into everywhere, like Elasticsearch, Redis, etc...
// MySQL must open row format for binlog
type Canal struct {
	m sync.Mutex

	cfg *Config

	master     *masterInfo
	dumper     *dump.Dumper
	dumpDoneCh chan struct{}
	syncer     *replication.BinlogSyncer

	rsLock     sync.Mutex
	rsHandlers []RowsEventHandler

	connLock sync.Mutex
	conn     *client.Conn

	wg sync.WaitGroup

	tableLock sync.Mutex
	tables    map[string]*schema.Table

	quit   chan struct{}
	closed sync2.AtomicBool
}

func NewCanal(cfg *Config) (*Canal, error) {
	c := new(Canal)
	c.cfg = cfg
	c.closed.Set(false)
	c.quit = make(chan struct{})

	os.MkdirAll(cfg.DataDir, 0755)

	c.dumpDoneCh = make(chan struct{})
	c.rsHandlers = make([]RowsEventHandler, 0, 4)
	c.tables = make(map[string]*schema.Table)

	var err error
	if c.master, err = loadMasterInfo(c.masterInfoPath()); err != nil {
		return nil, err
	} else if len(c.master.Addr) != 0 && c.master.Addr != c.cfg.Addr {
		log.Infof("MySQL addr %s in old master.info, but new %s, reset", c.master.Addr, c.cfg.Addr)
		// may use another MySQL, reset
		c.master = &masterInfo{}
	}

	c.master.Addr = c.cfg.Addr

	if c.dumper, err = dump.NewDumper(c.cfg.Dump.ExecutionPath, c.cfg.Addr, c.cfg.User, c.cfg.Password); err != nil {
		if err != exec.ErrNotFound {
			return nil, err
		}
		//no mysqldump, use binlog only
		c.dumper = nil
	}

	if err = c.prepareSyncer(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Canal) Start() error {
	if err := c.checkBinlogFormat(); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.run()

	return nil
}

func (c *Canal) run() error {
	defer c.wg.Done()

	if err := c.tryDump(); err != nil {
		log.Errorf("canal dump mysql err: %v", err)
		return err
	}

	close(c.dumpDoneCh)

	if err := c.startSyncBinlog(); err != nil {
		if !c.isClosed() {
			log.Errorf("canal start sync binlog err: %v", err)
		}
		return err
	}

	return nil
}

func (c *Canal) isClosed() bool {
	return c.closed.Get()
}

func (c *Canal) Close() {
	log.Infof("close canal")

	c.m.Lock()
	defer c.m.Unlock()

	if c.isClosed() {
		return
	}

	c.closed.Set(true)

	close(c.quit)

	c.connLock.Lock()
	c.conn.Close()
	c.conn = nil
	c.connLock.Unlock()

	if c.syncer != nil {
		c.syncer.Close()
		c.syncer = nil
	}

	c.master.Close()

	c.wg.Wait()
}

func (c *Canal) WaitDumpDone() <-chan struct{} {
	return c.dumpDoneCh
}

func (c *Canal) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	c.tableLock.Lock()
	t, ok := c.tables[key]
	c.tableLock.Unlock()

	if ok {
		return t, nil
	}

	t, err := schema.NewTable(c, db, table)
	if err != nil {
		return nil, err
	}

	c.tableLock.Lock()
	c.tables[key] = t
	c.tableLock.Unlock()

	return t, nil
}

func (c *Canal) checkBinlogFormat() error {
	res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return err
	} else if f, _ := res.GetString(0, 1); f != "ROW" {
		return fmt.Errorf("binlog must ROW format, but %s now", f)
	}

	// need to check MySQL binlog row image? full, minimal or noblob?
	// now only log
	if c.cfg.Flavor == mysql.MySQLFlavor {
		if res, err = c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_row_image"`); err != nil {
			return err
		}

		rowImage, _ := res.GetString(0, 1)
		log.Infof("MySQL use binlog row %s image", rowImage)
	}

	return nil
}

func (c *Canal) prepareSyncer() error {
	c.syncer = replication.NewBinlogSyncer(c.cfg.ServerID, c.cfg.Flavor)

	seps := strings.Split(c.cfg.Addr, ":")
	if len(seps) != 2 {
		return fmt.Errorf("invalid mysql addr format %s, must host:port", c.cfg.Addr)
	}

	port, err := strconv.ParseUint(seps[1], 10, 16)
	if err != nil {
		return err
	}

	if err = c.syncer.RegisterSlave(seps[0], uint16(port), c.cfg.User, c.cfg.Password); err != nil {
		return err
	}
	return nil
}

func (c *Canal) masterInfoPath() string {
	return path.Join(c.cfg.DataDir, "master.info")
}

// Execute a SQL
func (c *Canal) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if c.conn == nil {
			c.conn, err = client.Connect(c.cfg.Addr, c.cfg.User, c.cfg.Password, "")
			if err != nil {
				return nil, err
			}
		}

		rr, err = c.conn.Execute(cmd, args...)
		if err != nil && err != mysql.ErrBadConn {
			return
		} else if err == mysql.ErrBadConn {
			c.conn.Close()
			c.conn = nil
			continue
		} else {
			return
		}
	}
	return
}
