package mysql

import (
	"fmt"
	"strconv"
	"strings"
)

// Position for binlog filename + position based replication
type Position struct {
	Name string
	Pos  uint32
}

// Compare the position information between the p and o,
// if p > o return 1 means the position of p is further back than o.
// example is following
// id | project
// 1  | math
// 2  | cs
// 3  | ee
// p at id 3 and o at id 1.
// so this way we can determine from which position we should start syncing the data
// so that it is not consumed repeatedly.
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

// CompareBinlogFileName in this func, we'll compare the position between a and b by their filenames.
// if a>b will return 1.
// if b>a will return -1.
// the binlog filename consists of two parts (just like this --> [basename.00000x], for example bin_log.000001),
// one part is the `bin_log_basename` from the MySQL configuration file,
// and the other part is the number of the file's serial number, incrementing from 000001.
// you can use `show variables like 'log_%';` or `show variables like 'binlog%';` to show your configuration about binlog.
func CompareBinlogFileName(a, b string) int {
	// sometimes it's convenient to construct a `Position` literal with no `Name`
	if a == "" && b == "" {
		return 0
	} else if a == "" {
		return -1
	} else if b == "" {
		return 1
	}

	splitBinlogName := func(n string) (string, int) {
		// mysqld appends a numeric extension to the binary log base name to generate binary log file names
		// ...
		// If you supply an extension in the log name (for example, --log-bin=base_name.extension),
		// the extension is silently removed and ignored.
		// ref: https://dev.mysql.com/doc/refman/8.0/en/binary-log.html
		i := strings.LastIndexByte(n, '.')
		if i == -1 {
			// try keeping backward compatibility
			return n, 0
		}

		seq, err := strconv.Atoi(n[i+1:])
		if err != nil {
			panic(fmt.Sprintf("binlog file %s doesn't contain numeric extension", err))
		}
		return n[:i], seq
	}

	// get the basename(aBase) and the serial number(aSeq)
	aBase, aSeq := splitBinlogName(a)
	bBase, bSeq := splitBinlogName(b)

	// aBase and bBase generally will be equal if they are both from the same database configuration.
	if aBase > bBase {
		return 1
	} else if aBase < bBase {
		return -1
	}

	if aSeq > bSeq {
		return 1
	} else if aSeq < bSeq {
		return -1
	} else {
		return 0
	}
}
