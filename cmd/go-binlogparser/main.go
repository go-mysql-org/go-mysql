package main

import (
	"flag"
	"os"

	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	name   = flag.String("name", "", "binlog file name")
	offset = flag.Int64("offset", 0, "parse start offset")
	verify = flag.Bool("verify", false, "verify checksum")
)

func main() {
	flag.Parse()

	p := replication.NewBinlogParser()
	p.SetVerifyChecksum(*verify)

	f := func(e *replication.BinlogEvent) error {
		e.Dump(os.Stdout)
		return nil
	}

	err := p.ParseFile(*name, *offset, f)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}
