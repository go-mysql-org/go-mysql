// go-mysqlbinlog: a simple binlog tool to sync remote MySQL binlog.
// go-mysqlbinlog supports semi-sync mode like facebook mysqlbinlog.
// see http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL port")
var user = flag.String("user", "root", "MySQL user, must have replication privilege")
var password = flag.String("password", "", "MySQL password")

var flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

var file = flag.String("file", "", "Binlog filename")
var pos = flag.Int("pos", 4, "Binlog position")

var semiSync = flag.Bool("semisync", false, "Support semi sync")
var backupPath = flag.String("backup_path", "", "backup path to store binlog files")

var rawMode = flag.Bool("raw", false, "Use raw mode")

func main() {
	flag.Parse()

	b := replication.NewBinlogSyncer(101, *flavor)

	if err := b.RegisterSlave(*host, uint16(*port), *user, *password); err != nil {
		fmt.Printf("Register slave error: %v \n", errors.ErrorStack(err))
		return
	}

	b.SetRawMode(*rawMode)

	if *semiSync {
		if err := b.EnableSemiSync(); err != nil {
			fmt.Printf("Enable semi sync replication mode err: %v\n", errors.ErrorStack(err))
			return
		}
	}

	pos := mysql.Position{*file, uint32(*pos)}
	if len(*backupPath) > 0 {
		// must raw mode
		b.SetRawMode(true)

		err := b.StartBackup(*backupPath, pos, 0)
		if err != nil {
			fmt.Printf("Start backup error: %v\n", errors.ErrorStack(err))
			return
		}
	} else {
		s, err := b.StartSync(pos)
		if err != nil {
			fmt.Printf("Start sync error: %v\n", errors.ErrorStack(err))
			return
		}

		for {
			e, err := s.GetEvent()
			if err != nil {
				fmt.Printf("Get event error: %v\n", errors.ErrorStack(err))
				return
			}

			e.Dump(os.Stdout)
		}
	}

}
