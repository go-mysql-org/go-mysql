// go-mysqlbinlog: a simple binlog tool to sync remote MySQL binlog.
// go-mysqlbinlog supports semi-sync mode like facebook mysqlbinlog.
// see http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	host     = flag.String("host", "127.0.0.1", "MySQL host")
	port     = flag.Int("port", 3306, "MySQL port")
	user     = flag.String("user", "root", "MySQL user, must have replication privilege")
	password = flag.String("password", "", "MySQL password")

	flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

	file = flag.String("file", "", "Binlog filename")
	pos  = flag.Int("pos", 4, "Binlog position")
	gtid = flag.String("gtid", "", "Binlog GTID set that this slave has executed")

	semiSync   = flag.Bool("semisync", false, "Support semi sync")
	backupPath = flag.String("backup_path", "", "backup path to store binlog files")

	rawMode = flag.Bool("raw", false, "Use raw mode")
)

func main() {
	flag.Parse()

	cfg := replication.BinlogSyncerConfig{
		ServerID: 101,
		Flavor:   *flavor,

		Host:            *host,
		Port:            uint16(*port),
		User:            *user,
		Password:        *password,
		RawModeEnabled:  *rawMode,
		SemiSyncEnabled: *semiSync,
		UseDecimal:      true,
	}

	b := replication.NewBinlogSyncer(cfg)

	pos := mysql.Position{Name: *file, Pos: uint32(*pos)}
	if len(*backupPath) > 0 {
		// Backup will always use RawMode.
		err := b.StartBackup(*backupPath, pos, 0)
		if err != nil {
			fmt.Printf("Start backup error: %v\n", errors.ErrorStack(err))
			return
		}
	} else {
		var (
			s   *replication.BinlogStreamer
			err error
		)
		if len(*gtid) > 0 {
			gset, err := mysql.ParseGTIDSet(*flavor, *gtid)
			if err != nil {
				fmt.Printf("Failed to parse gtid %s with flavor %s, error: %v\n",
					*gtid, *flavor, errors.ErrorStack(err))
			}
			s, err = b.StartSyncGTID(gset)
			if err != nil {
				fmt.Printf("Start sync by GTID error: %v\n", errors.ErrorStack(err))
				return
			}
		} else {
			s, err = b.StartSync(pos)
			if err != nil {
				fmt.Printf("Start sync error: %v\n", errors.ErrorStack(err))
				return
			}
		}

		for {
			e, err := s.GetEvent(context.Background())
			if err != nil {
				// Try to output all left events
				events := s.DumpEvents()
				for _, e := range events {
					e.Dump(os.Stdout)
				}
				fmt.Printf("Get event error: %v\n", errors.ErrorStack(err))
				return
			}

			e.Dump(os.Stdout)
		}
	}
}
