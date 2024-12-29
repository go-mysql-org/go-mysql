package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

var (
	host     = flag.String("host", "127.0.0.1", "MySQL host")
	port     = flag.Int("port", 3306, "MySQL port")
	user     = flag.String("user", "root", "MySQL user, must have replication privilege")
	password = flag.String("password", "", "MySQL password")

	flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

	serverID  = flag.Int("server-id", 101, "Unique Server ID")
	mysqldump = flag.String("mysqldump", "mysqldump", "mysqldump execution path")

	dbs          = flag.String("dbs", "test", "dump databases, separated by comma")
	tables       = flag.String("tables", "", "dump tables, separated by comma, will overwrite dbs")
	tableDB      = flag.String("table_db", "test", "database for dump tables")
	ignoreTables = flag.String("ignore_tables", "", "ignore tables, must be database.table format, separated by comma")

	startName = flag.String("bin_name", "", "start sync from binlog name")
	startPos  = flag.Uint("bin_pos", 0, "start sync from binlog position of")

	heartbeatPeriod = flag.Duration("heartbeat", 60*time.Second, "master heartbeat period")
	readTimeout     = flag.Duration("read_timeout", 90*time.Second, "connection read timeout")
)

func main() {
	flag.Parse()

	err := mysql.ValidateFlavor(*flavor)
	if err != nil {
		fmt.Printf("Flavor error: %v\n", errors.ErrorStack(err))
		return
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = net.JoinHostPort(*host, strconv.Itoa(*port))
	cfg.User = *user
	cfg.Password = *password
	cfg.Flavor = *flavor
	cfg.UseDecimal = true

	cfg.ReadTimeout = *readTimeout
	cfg.HeartbeatPeriod = *heartbeatPeriod
	cfg.ServerID = uint32(*serverID)
	cfg.Dump.ExecutionPath = *mysqldump
	cfg.Dump.DiscardErr = false

	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("create canal err %v", err)
		os.Exit(1)
	}

	if len(*ignoreTables) > 0 {
		subs := strings.Split(*ignoreTables, ",")
		for _, sub := range subs {
			if seps := strings.Split(sub, "."); len(seps) == 2 {
				c.AddDumpIgnoreTables(seps[0], seps[1])
			}
		}
	}

	if len(*tables) > 0 && len(*tableDB) > 0 {
		subs := strings.Split(*tables, ",")
		c.AddDumpTables(*tableDB, subs...)
	} else if len(*dbs) > 0 {
		subs := strings.Split(*dbs, ",")
		c.AddDumpDatabases(subs...)
	}

	c.SetEventHandler(&handler{})

	startPos := mysql.Position{
		Name: *startName,
		Pos:  uint32(*startPos),
	}

	go func() {
		err = c.RunFrom(startPos)
		if err != nil {
			fmt.Printf("start canal err %v", err)
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sc

	c.Close()
}

type handler struct {
	canal.DummyEventHandler
}

func (h *handler) OnRow(e *canal.RowsEvent) error {
	fmt.Printf("%v\n", e)

	return nil
}

func (h *handler) String() string {
	return "TestHandler"
}
