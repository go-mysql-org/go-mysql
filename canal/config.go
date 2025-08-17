package canal

import (
	"crypto/tls"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
)

type DumpConfig struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	// If not set, ignore using mysqldump.
	ExecutionPath string `toml:"mysqldump"`

	// Will override Databases, tables is in database table_db
	Tables  []string `toml:"tables"`
	TableDB string   `toml:"table_db"`

	Databases []string `toml:"dbs"`

	// Ignore table format is db.table
	IgnoreTables []string `toml:"ignore_tables"`

	// Dump only selected records. Quotes are mandatory
	Where string `toml:"where"`

	// If true, discard error msg, else, output to stderr
	DiscardErr bool `toml:"discard_err"`

	// Set true to skip --master-data if we have no privilege to do
	// 'FLUSH TABLES WITH READ LOCK'
	SkipMasterData bool `toml:"skip_master_data"`

	// Set to change the default max_allowed_packet size
	MaxAllowedPacketMB int `toml:"max_allowed_packet_mb"`

	// Set to change the default protocol to connect with
	Protocol string `toml:"protocol"`

	// Set extra options
	ExtraOptions []string `toml:"extra_options"`
}

type Config struct {
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`

	Charset         string        `toml:"charset"`
	ServerID        uint32        `toml:"server_id"`
	Flavor          string        `toml:"flavor"`
	HeartbeatPeriod time.Duration `toml:"heartbeat_period"`
	ReadTimeout     time.Duration `toml:"read_timeout"`

	// IncludeTableRegex or ExcludeTableRegex should contain database name.
	// IncludeTableRegex defines the tables that will be included, if empty, all tables will be included.
	// ExcludeTableRegex defines the tables that will be excluded from the ones defined by IncludeTableRegex.
	// Only a table which matches IncludeTableRegex and dismatches ExcludeTableRegex will be processed
	// eg, IncludeTableRegex : [".*\\.canal"], ExcludeTableRegex : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'.
	// Default IncludeTableRegex and ExcludeTableRegex are empty, this will include all tables
	IncludeTableRegex []string `toml:"include_table_regex"`
	ExcludeTableRegex []string `toml:"exclude_table_regex"`

	// discard row event without table meta
	DiscardNoMetaRowEvent bool `toml:"discard_no_meta_row_event"`

	Dump DumpConfig `toml:"dump"`

	UseDecimal bool `toml:"use_decimal"`
	ParseTime  bool `toml:"parse_time"`

	TimestampStringLocation *time.Location

	// SemiSyncEnabled enables semi-sync or not.
	SemiSyncEnabled bool `toml:"semi_sync_enabled"`

	// maximum number of attempts to re-establish a broken connection, zero or negative number means infinite retry.
	// this configuration will not work if DisableRetrySync is true
	MaxReconnectAttempts int `toml:"max_reconnect_attempts"`

	// whether disable re-sync for broken connection
	DisableRetrySync bool `toml:"disable_retry_sync"`

	// whether the function WaitUntilPos() can use FLUSH BINARY LOGS
	// to ensure we advance past a position. This should not strictly be required,
	// and requires additional privileges.
	DisableFlushBinlogWhileWaiting bool `toml:"disable_flush_binlog_while_waiting"`

	// Set TLS config
	TLSConfig *tls.Config

	// Set Logger
	Logger *slog.Logger

	// Set Dialer
	Dialer client.Dialer

	// Set the hostname that is used when registering as replica. This is similar to `report_host` in MySQL.
	// This will be truncated if it is longer than 255 characters.
	Localhost string

	// EventCacheCount is the capacity of the BinlogStreamer internal event channel.
	// the default value is 10240.
	// if you table contain large columns, you can decrease this value to avoid OOM.
	EventCacheCount int

	// FillZeroLogPos enables dynamic LogPos calculation for MariaDB.
	// When enabled, automatically adds BINLOG_SEND_ANNOTATE_ROWS_EVENT flag
	// to ensure correct position calculation in MariaDB 11.4+.
	// Only works with MariaDB flavor.
	FillZeroLogPos bool
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

// NewDefaultConfig initiates some default config for Canal
func NewDefaultConfig() *Config {
	c := new(Config)

	c.Addr = mysql.DEFAULT_ADDR
	c.User = mysql.DEFAULT_USER
	c.Password = mysql.DEFAULT_PASSWORD
	c.Charset = mysql.DEFAULT_CHARSET
	c.ServerID = uint32(rand.New(rand.NewSource(utils.Now().Unix())).Intn(1000)) + 1001
	c.Flavor = mysql.DEFAULT_FLAVOR

	c.Dump.ExecutionPath = mysql.DEFAULT_DUMP_EXECUTION_PATH
	c.Dump.DiscardErr = true
	c.Dump.SkipMasterData = false

	c.Logger = slog.Default()

	dialer := &net.Dialer{}
	c.Dialer = dialer.DialContext

	return c
}
