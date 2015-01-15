package main

import (
	"flag"
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"os"
)

var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL port")
var user = flag.String("user", "root", "MySQL user")
var password = flag.String("password", "", "MySQL password")

var flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

var file = flag.String("file", "", "Binlog filename")
var pos = flag.Int("pos", 4, "Binlog position")

func main() {
	flag.Parse()

	b := replication.NewBinlogSyncer(101, *flavor)

	if err := b.RegisterSlave(*host, uint16(*port), *user, *password); err != nil {
		fmt.Printf("Register slave error: %v \n", err)
		return
	}

	s, err := b.StartSync(mysql.Position{*file, uint32(*pos)})
	if err != nil {
		fmt.Printf("Start sync error: %v\n", err)
		return
	}

	for {
		e, err := s.GetEvent()
		if err != nil {
			fmt.Printf("Get event error: %v\n", err)
			return
		}

		e.Dump(os.Stdout)
	}
}
