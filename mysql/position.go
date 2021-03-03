package mysql

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// For binlog filename + position based replication
type Position struct {
	Name string
	Pos  uint32
}

func (p Position) Compare(o Position) int {
	// First compare binlog name
	nameCmp := CompareBinlogFileName(p.Name, o.Name)
	if nameCmp != 0 {
		return nameCmp
	}
	// Same binlog file, compare position
	if p.Pos > o.Pos {
		return 1
	} else if p.Pos < o.Pos {
		return -1
	} else {
		return 0
	}
}

func (p Position) String() string {
	return fmt.Sprintf("(%s, %d)", p.Name, p.Pos)
}

func CompareBinlogFileName(a, b string) int {
	// sometimes it's convenient to construct a `Position` literal with no `Name`
	if a == "" && b == "" {
		return 0
	}
	if a == "" {
		return -1
	}
	if b == "" {
		return 1
	}

	// mysqld appends a numeric extension to the binary log base name to generate binary log file names
	// ref: https://dev.mysql.com/doc/refman/8.0/en/binary-log.html
	aNum, _ := strconv.Atoi(strings.TrimLeft(filepath.Ext(a)[1:], "0"))
	bNum, _ := strconv.Atoi(strings.TrimLeft(filepath.Ext(b)[1:], "0"))

	if aNum > bNum {
		return 1
	} else if aNum < bNum {
		return -1
	} else {
		return 0
	}
}
