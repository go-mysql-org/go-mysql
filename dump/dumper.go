package dump

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// Unlike mysqldump, Dumper is designed for parsing and syning data easily.
type Dumper struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	ExecutionPath string

	Addr     string
	User     string
	Password string
	Protocol string

	// Will override Databases
	Tables  []string
	TableDB string

	Databases []string

	Where   string
	Charset string

	IgnoreTables map[string][]string

	ExtraOptions []string

	ErrOut io.Writer

	masterDataSkipped bool
	maxAllowedPacket  int
	hexBlob           bool

	// see detectColumnStatisticsParamSupported
	isColumnStatisticsParamSupported bool

	mysqldumpVersion    string
	sourceDataSupported bool

	Logger *slog.Logger
}

func NewDumper(executionPath string, addr string, user string, password string) (*Dumper, error) {
	var path string
	var err error

	if len(executionPath) == 0 { // No explicit path set
		path, err = exec.LookPath("mysqldump")
		if err != nil {
			path, err = exec.LookPath("mariadb-dump")
			if err != nil {
				// Using a new error as `err` will only mention mariadb-dump and not mysqldump
				return nil, errors.New("not able to find mysqldump or mariadb-dump in path")
			}
		}
	} else {
		path, err = exec.LookPath(executionPath)
		if err != nil {
			return nil, err
		}
	}

	d := new(Dumper)
	d.ExecutionPath = path
	d.Addr = addr
	d.User = user
	d.Password = password
	d.Tables = make([]string, 0, 16)
	d.Databases = make([]string, 0, 16)
	d.Charset = mysql.DEFAULT_CHARSET
	d.IgnoreTables = make(map[string][]string)
	d.ExtraOptions = make([]string, 0, 5)
	d.masterDataSkipped = false

	out, err := exec.Command(d.ExecutionPath, `--help`).CombinedOutput()
	if err != nil {
		return d, err
	}
	d.isColumnStatisticsParamSupported = d.detectColumnStatisticsParamSupported(out)
	d.mysqldumpVersion = d.getMysqldumpVersion(out)
	d.sourceDataSupported = d.detectSourceDataSupported(d.mysqldumpVersion)

	d.ErrOut = os.Stderr

	d.Logger = slog.Default()

	return d, nil
}

// New mysqldump versions try to send queries to information_schema.COLUMN_STATISTICS table which does not exist in old MySQL (<5.x).
// And we got error: "Unknown table 'COLUMN_STATISTICS' in information_schema (1109)".
//
// mysqldump may not send this query if it is started with parameter --column-statistics.
// But this parameter exists only for versions >=8.0.2 (https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-2.html).
//
// For environments where the version of mysql-server and mysqldump differs, we try to check this parameter and use it if available.
func (d *Dumper) detectColumnStatisticsParamSupported(helpOutput []byte) bool {
	return bytes.Contains(helpOutput, []byte(`--column-statistics`))
}

// mysqldump  Ver 10.19 Distrib 10.3.37-MariaDB, for linux-systemd (x86_64)`, `10.3.37-MariaDB
// opt/mysql/11.0.0/bin/mysqldump from 11.0.0-preview-MariaDB, client 10.19 for linux-systemd (x86_64)
func (d *Dumper) getMysqldumpVersion(helpOutput []byte) string {
	mysqldumpVersionRegexpNew := regexp.MustCompile(`mysqldump  Ver ([0-9][^ ]*) for`)
	if m := mysqldumpVersionRegexpNew.FindSubmatch(helpOutput); m != nil {
		return string(m[1])
	}

	mysqldumpVersionRegexpOld := regexp.MustCompile(`mysqldump  Ver .* Distrib ([0-9][^ ]*),`)
	if m := mysqldumpVersionRegexpOld.FindSubmatch(helpOutput); m != nil {
		return string(m[1])
	}

	mysqldumpVersionRegexpMaria := regexp.MustCompile(`mysqldump from ([0-9][^ ]*), `)
	if m := mysqldumpVersionRegexpMaria.FindSubmatch(helpOutput); m != nil {
		return string(m[1])
	}

	return ""
}

func (d *Dumper) detectSourceDataSupported(version string) bool {
	// Failed to detect mysqldump version
	if version == "" {
		return false
	}

	// MySQL 5.x
	if version[0] == byte('5') {
		return false
	}

	if strings.Contains(version, "MariaDB") {
		return false
	}

	return true
}

func (d *Dumper) SetCharset(charset string) {
	d.Charset = charset
}

func (d *Dumper) SetProtocol(protocol string) {
	d.Protocol = protocol
}

func (d *Dumper) SetWhere(where string) {
	d.Where = where
}

func (d *Dumper) SetExtraOptions(options []string) {
	d.ExtraOptions = options
}

func (d *Dumper) SetErrOut(o io.Writer) {
	d.ErrOut = o
}

// SkipMasterData: In some cloud MySQL, we have no privilege to use `--master-data`.
func (d *Dumper) SkipMasterData(v bool) {
	d.masterDataSkipped = v
}

func (d *Dumper) SetMaxAllowedPacket(i int) {
	d.maxAllowedPacket = i
}

func (d *Dumper) SetHexBlob(v bool) {
	d.hexBlob = v
}

func (d *Dumper) AddDatabases(dbs ...string) {
	d.Databases = append(d.Databases, dbs...)
}

func (d *Dumper) AddTables(db string, tables ...string) {
	if d.TableDB != db {
		d.TableDB = db
		d.Tables = d.Tables[0:0]
	}

	d.Tables = append(d.Tables, tables...)
}

func (d *Dumper) AddIgnoreTables(db string, tables ...string) {
	t := d.IgnoreTables[db]
	t = append(t, tables...)
	d.IgnoreTables[db] = t
}

func (d *Dumper) Reset() {
	d.Tables = d.Tables[0:0]
	d.TableDB = ""
	d.IgnoreTables = make(map[string][]string)
	d.Databases = d.Databases[0:0]
	d.Where = ""
}

func (d *Dumper) Dump(w io.Writer) error {
	args := make([]string, 0, 16)

	// Common args
	if strings.Contains(d.Addr, "/") {
		args = append(args, fmt.Sprintf("--socket=%s", d.Addr))
	} else {
		host, port, err := net.SplitHostPort(d.Addr)
		if err != nil {
			host = d.Addr
		}

		args = append(args, fmt.Sprintf("--host=%s", host))
		if port != "" {
			args = append(args, fmt.Sprintf("--port=%s", port))
		}
	}

	args = append(args, fmt.Sprintf("--user=%s", d.User))
	passwordArg := fmt.Sprintf("--password=%s", d.Password)
	args = append(args, passwordArg)
	passwordArgIndex := len(args) - 1

	if !d.masterDataSkipped {
		if d.sourceDataSupported {
			args = append(args, "--source-data")
		} else {
			args = append(args, "--master-data")
		}
	}

	if d.maxAllowedPacket > 0 {
		// mysqldump param should be --max-allowed-packet=%dM not be --max_allowed_packet=%dM
		args = append(args, fmt.Sprintf("--max-allowed-packet=%dM", d.maxAllowedPacket))
	}

	if d.Protocol != "" {
		args = append(args, fmt.Sprintf("--protocol=%s", d.Protocol))
	}

	args = append(args, "--single-transaction")
	args = append(args, "--skip-lock-tables")

	// Disable uncessary data
	args = append(args, "--compact")
	args = append(args, "--skip-opt")
	args = append(args, "--quick")

	// We only care about data
	args = append(args, "--no-create-info")

	// Multi row is easy for us to parse the data
	args = append(args, "--skip-extended-insert")
	args = append(args, "--skip-tz-utc")
	if d.hexBlob {
		// Use hex for the binary type
		args = append(args, "--hex-blob")
	}

	for db, tables := range d.IgnoreTables {
		for _, table := range tables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", db, table))
		}
	}

	if len(d.Charset) != 0 {
		args = append(args, fmt.Sprintf("--default-character-set=%s", d.Charset))
	}

	if len(d.Where) != 0 {
		args = append(args, fmt.Sprintf("--where=%s", d.Where))
	}

	if len(d.ExtraOptions) != 0 {
		args = append(args, d.ExtraOptions...)
	}

	if d.isColumnStatisticsParamSupported {
		args = append(args, `--column-statistics=0`)
	}

	if len(d.Tables) == 0 && len(d.Databases) == 0 {
		args = append(args, "--all-databases")
	} else if len(d.Tables) == 0 {
		args = append(args, "--databases")
		args = append(args, d.Databases...)
	} else {
		args = append(args, d.TableDB)
		args = append(args, d.Tables...)

		// If we only dump some tables, the dump data will not have database name
		// which makes us hard to parse, so here we add it manually.

		_, err := fmt.Fprintf(w, "USE `%s`;\n", d.TableDB)
		if err != nil {
			return fmt.Errorf(`could not write USE command: %w`, err)
		}
	}

	args[passwordArgIndex] = "--password=******"
	d.Logger.Info("exec mysqldump with", slog.Any("args", args))
	args[passwordArgIndex] = passwordArg
	cmd := exec.Command(d.ExecutionPath, args...)

	cmd.Stderr = d.ErrOut
	cmd.Stdout = w

	return cmd.Run()
}

// DumpAndParse: Dump MySQL and parse immediately
func (d *Dumper) DumpAndParse(h ParseHandler) error {
	r, w := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := Parse(r, h, !d.masterDataSkipped)
		_ = r.CloseWithError(err)
		done <- err
	}()

	err := d.Dump(w)
	_ = w.CloseWithError(err)

	err = <-done

	return errors.Trace(err)
}
