package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"golang.org/x/net/context"
)

var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL port")
var user = flag.String("user", "root", "MySQL user, must have replication privilege")
var password = flag.String("password", "", "MySQL password")

var flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

var serverID = flag.Int("server-id", 101, "Unique Server ID")
var mysqldump = flag.String("mysqldump", "mysqldump", "mysqldump execution path")

var dbs = flag.String("dbs", "test", "dump databases, seperated by comma")
var tables = flag.String("tables", "", "dump tables, seperated by comma, will overwrite dbs")
var tableDB = flag.String("table_db", "test", "database for dump tables")
var ignoreTables = flag.String("ignore_tables", "", "ignore tables, must be database.table format, separated by comma")

var startName = flag.String("bin_name", "", "start sync from binlog name")
var startPos = flag.Uint("bin_pos", 0, "start sync from binlog position of")

func main() {
	flag.Parse()

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = *user
	cfg.Password = *password
	cfg.Flavor = *flavor

	cfg.ServerID = uint32(*serverID)
	cfg.Dump.ExecutionPath = *mysqldump
	cfg.Dump.DiscardErr = false

	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("create canal err %v", err)
		os.Exit(1)
	}

	if len(*ignoreTables) == 0 {
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
		*startName,
		uint32(*startPos),
	}

	err = c.StartFrom(startPos)
	if err != nil {
		fmt.Printf("start canal err %V", err)
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
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

func (h *handler) OnRow(_ context.Context, e *canal.RowsEvent) error {
	fmt.Printf("%v\n", e)

	return nil
}

func (h *handler) String() string {
	return "TestHandler"
}
