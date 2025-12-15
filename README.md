# go-mysql

A pure Go library to handle MySQL network protocol and replication as used by MySQL and MariaDB.

![semver](https://img.shields.io/github/v/tag/go-mysql-org/go-mysql)
![example workflow](https://github.com/go-mysql-org/go-mysql/actions/workflows/ci.yml/badge.svg)
![gomod version](https://img.shields.io/github/go-mod/go-version/go-mysql-org/go-mysql/master)
[![Go Reference](https://pkg.go.dev/badge/github.com/go-mysql-org/go-mysql.svg)](https://pkg.go.dev/github.com/go-mysql-org/go-mysql)

## Platform Support

As a pure Go library, this project follows [Go's minimum requirements](https://go.dev/wiki/MinimumRequirements).

This library has been tested or deployed on the following operating systems and architectures:

| Operating System | Architecture | Runtime Supported | CI | Notes                                                                                                                          |
|------------------|--------------|-------------------|----|--------------------------------------------------------------------------------------------------------------------------------|
| Linux            | amd64        | ✅                | ✅ | Check GitHub Actions of this project.                                                                                          |
| Linux            | s390x        | ✅                | ✅ | A daily CI runs on an s390x VM, supported by the [IBM Z and LinuxONE Community](https://www.ibm.com/community/z/open-source/). |
| Linux            | arm64        | ✅                | ❌ | Deployed in a production environment of a user.                                                                                |
| Linux            | arm          | ✅                | ❌ | A test in CI to make sure builds for 32-bits platforms work.                                                                   |
| FreeBSD          | amd64        | ✅                | ❌ | Sporadically tested by developers.                                                                                             |

Other platforms supported by Go may also work, but they have not been verified. Feel free to report your test results.

This library is not compatible with [TinyGo](https://tinygo.org/).

## Changelog

This library uses [Changelog](CHANGELOG.md).

---
# Content
* [Replication](#replication) - Process events from a binlog stream.
* [Incremental dumping](#canal) - Sync from MySQL to Redis, Elasticsearch, etc.
* [Client](#client) - Simple MySQL client.
* [Fake server](#server) - server side of the MySQL protocol, as library.
* [database/sql like driver](#driver) - An alternative `database/sql` driver for MySQL.
* [Logging](#logging) - Custom logging options.
* [Migration](#how-to-migrate-to-this-repo) - Information for how to migrate if you used the old location of this project.

## Examples

The `cmd` directory contains example applications that can be build by running `make build` in the root of the project. The resulting binaries will be places in `bin/`.

- `go-binlogparser`: parses a binlog file at a given offset
- `go-canal`: streams binlog events from a server to canal
- `go-mysqlbinlog`: streams binlog events
- `go-mysqldump`: like `mysqldump`, but in Go
- `go-mysqlserver`: fake MySQL server

## Replication

Replication package handles MySQL replication protocol like [python-mysql-replication](https://github.com/noplay/python-mysql-replication).

You can use it as a MySQL replica to sync binlog from master then do something, like updating cache, etc...

### Example

```go
import (
	"github.com/go-mysql-org/go-mysql/replication"
	"os"
)
// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
// flavor is mysql or mariadb
cfg := replication.BinlogSyncerConfig {
	ServerID: 100,
	Flavor:   "mysql",
	Host:     "127.0.0.1",
	Port:     3306,
	User:     "root",
	Password: "",
}
syncer := replication.NewBinlogSyncer(cfg)

// Start sync with specified binlog file and position
streamer, _ := syncer.StartSync(mysql.Position{binlogFile, binlogPos})

// or you can start a gtid replication like
// gtidSet, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
// streamer, _ := syncer.StartSyncGTID(gtidSet)
// the mysql GTID set is like this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2" and uses mysql.MySQLFlavor
// the mariadb GTID set is like this "0-1-100" and uses mysql.MariaDBFlavor

for {
	ev, _ := streamer.GetEvent(context.Background())
	// Dump event
	ev.Dump(os.Stdout)
}

// or we can use a timeout context
for {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	ev, err := streamer.GetEvent(ctx)
	cancel()

	if err == context.DeadlineExceeded {
		// meet timeout
		continue
	}

	ev.Dump(os.Stdout)
}
```

The output looks:

```
=== RotateEvent ===
Date: 1970-01-01 08:00:00
Log position: 0
Event size: 43
Position: 4
Next log name: mysql.000002

=== FormatDescriptionEvent ===
Date: 2014-12-18 16:36:09
Log position: 120
Event size: 116
Version: 4
Server version: 5.6.19-log
Create date: 2014-12-18 16:36:09

=== QueryEvent ===
Date: 2014-12-18 16:38:24
Log position: 259
Event size: 139
Salve proxy ID: 1
Execution time: 0
Error code: 0
Schema: test
Query: DROP TABLE IF EXISTS `test_replication` /* generated by server */
```

### MariaDB 11.4+ compatibility

MariaDB 11.4+ introduced an optimization where events written through transaction or statement cache have `LogPos=0` so they can be copied directly to the binlog without computing the real end position. This optimization improves performance but makes position tracking unreliable for replication clients that need to track LogPos of events inside transactions.

To address this, a `FillZeroLogPos` configuration option is available:

```go
cfg := replication.BinlogSyncerConfig {
	ServerID: 100,
	Flavor:   "mariadb",
	Host:     "127.0.0.1",
	Port:     3306,
	User:     "root",
	Password: "",
	// Enable dynamic LogPos calculation for MariaDB 11.4+
	FillZeroLogPos: true,
}
```

**Behavior:**
- When `FillZeroLogPos` is `true` and flavor is `mariadb`, the library automatically:
  - Adds `BINLOG_SEND_ANNOTATE_ROWS_EVENT` flag to binlog dump commands. This ensures correct position tracking by making the server send `ANNOTATE_ROWS_EVENT` events which are needed for accurate position calculation.
  - Calculates LogPos dynamically for events with `LogPos=0` that are not artificial.
- Only works with MariaDB flavor; has no effect with MySQL.
- Should be set to `true` if tracking of LogPos inside transactions is required.

## Canal

Canal is a package that can sync your MySQL into everywhere, like Redis, Elasticsearch.

First, canal will dump your MySQL data then sync changed data using binlog incrementally.

You must use ROW format for binlog, full binlog row image is preferred, because we may meet some errors when primary key changed in update for minimal or noblob row image.

A simple example:

```go
package main

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

type MyEventHandler struct {
	canal.DummyEventHandler
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	log.Infof("%s %v\n", e.Action, e.Rows)
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func main() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	// We only care table canal_test in test db
	cfg.Dump.TableDB = "test"
	cfg.Dump.Tables = []string{"canal_test"}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&MyEventHandler{})

	// Start canal
	c.Run()
}
```

You can see [go-mysql-elasticsearch](https://github.com/go-mysql-org/go-mysql-elasticsearch) for how to sync MySQL data into Elasticsearch.

## Client

Client package supports a simple MySQL connection driver which you can use it to communicate with MySQL server.

For an example see [`example_client_test.go`](client/example_client_test.go). You can run this testable example with
`go test -v ./client -run Example`.

Tested MySQL versions for the client include:
- 5.5.x
- 5.6.x
- 5.7.x
- 8.0.x

### Example for SELECT streaming (v1.1.1)
You can use also streaming for large SELECT responses.
The callback function will be called for every result row without storing the whole resultset in memory.
`result.Fields` will be filled before the first callback call.

```go
// ...
var result mysql.Result
err := conn.ExecuteSelectStreaming(`select id, name from table LIMIT 100500`, &result, func(row []mysql.FieldValue) error {
    for idx, val := range row {
    	field := result.Fields[idx]
    	// You must not save FieldValue.AsString() value after this callback is done.
    	// Copy it if you need.
    	// ...
    }
    return nil
}, nil)

// ...
```

### Example for connection pool (v1.3.0)

```go
import (
    "github.com/go-mysql-org/go-mysql/client"
)

pool := client.NewPool(log.Debugf, 100, 400, 5, "127.0.0.1:3306", `root`, ``, `test`)
// ...
conn, _ := pool.GetConn(ctx)
defer pool.PutConn(conn)

conn.Execute() / conn.Begin() / etc...
```

## Server

Server package supplies a framework to implement a simple MySQL server which can handle the packets from the MySQL client.
You can use it to build your own MySQL proxy. The server connection is compatible with MySQL 5.5, 5.6, 5.7, and 8.0 versions,
so that most MySQL clients should be able to connect to the Server without modifications.

### Example

Minimalistic MySQL server implementation:

```go
package main

import (
	"log"
	"net"

	"github.com/go-mysql-org/go-mysql/server"
)

func main() {
	// Listen for connections on localhost port 4000
	l, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		log.Fatal(err)
	}

	// Accept a new connection once
	c, err := l.Accept()
	if err != nil {
		log.Fatal(err)
	}

	// Create a connection with user root and an empty password.
	// You can use your own handler to handle command here.
	conn, err := server.NewConn(c, "root", "", server.EmptyHandler{})
	if err != nil {
		log.Fatal(err)
	}

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			log.Fatal(err)
		}
	}
}
```

Another shell

```
$ mysql -h127.0.0.1 -P4000 -uroot
Your MySQL connection id is 10001
Server version: 5.7.0

MySQL [(none)]>
// Since EmptyHandler implements no commands, it will throw an error on any query that you will send
```

> ```NewConn()``` will use default server configurations:
> 1. automatically generate default server certificates and enable TLS/SSL support.
> 2. support three mainstream authentication methods **'mysql_native_password'**, **'caching_sha2_password'**, and **'sha256_password'**
>    and use **'mysql_native_password'** as default.
> 3. use an in-memory user credential provider to store user and password.
>
> To customize server configurations, use ```NewServer()``` and create connection via ```NewCustomizedConn()```.

## Driver

Driver is the package that you can use go-mysql with go database/sql like other drivers. A simple example:

```go
package main

import (
	"database/sql"

	_ "github.com/go-mysql-org/go-mysql/driver"
)

func main() {
	// dsn format: "user:password@addr?dbname"
	dsn := "root@127.0.0.1:3306?test"
	db, _ := sql.Open("mysql", dsn)
	db.Close()
}
```

### Driver Options

Configuration options can be provided by the standard DSN (Data Source Name).

```
[user[:password]@]addr[/db[?param=X]]
```

#### `collation`

Set a collation during the Auth handshake.

| Type      | Default         | Example                                               |
| --------- | --------------- | ----------------------------------------------------- |
| string    | utf8_general_ci | user:pass@localhost/mydb?collation=latin1_general_ci  |

#### `compress`

Enable compression between the client and the server. Valid values are 'zstd','zlib','uncompressed'.

| Type      | Default       | Example                                 |
| --------- | ------------- | --------------------------------------- |
| string    | uncompressed  | user:pass@localhost/mydb?compress=zlib  |

#### `readTimeout`

I/O read timeout. The time unit is specified in the argument value using
golang's [ParseDuration](https://pkg.go.dev/time#ParseDuration) format.

0 means no timeout.

| Type      | Default   | Example                                     |
| --------- | --------- | ------------------------------------------- |
| duration  | 0         | user:pass@localhost/mydb?readTimeout=10s    |

#### `ssl`

Enable TLS between client and server. Valid values are `true` or `custom`. When using `custom`,
the connection will use the TLS configuration set by SetCustomTLSConfig matching the host.

| Type      | Default   | Example                                     |
| --------- | --------- | ------------------------------------------- |
| string    |           | user:pass@localhost/mydb?ssl=true           |

#### `timeout`

Timeout is the maximum amount of time a dial will wait for a connect to complete.
The time unit is specified in the argument value using golang's [ParseDuration](https://pkg.go.dev/time#ParseDuration) format.

0 means no timeout.

| Type      | Default   | Example                                     |
| --------- | --------- | ------------------------------------------- |
| duration  | 0         | user:pass@localhost/mydb?timeout=1m         |

#### `writeTimeout`

I/O write timeout. The time unit is specified in the argument value using
golang's [ParseDuration](https://pkg.go.dev/time#ParseDuration) format.

0 means no timeout.

| Type      | Default   | Example                                         |
| --------- | --------- | ----------------------------------------------- |
| duration  | 0         | user:pass@localhost/mydb?writeTimeout=1m30s     |

#### `retries`

Allows disabling the golang `database/sql` default behavior to retry errors
when `ErrBadConn` is returned by the driver. When retries are disabled
this driver will not return `ErrBadConn` from the `database/sql` package.

Valid values are `on` (default) and `off`.

| Type      | Default   | Example                                         |
| --------- | --------- | ----------------------------------------------- |
| string    | on        | user:pass@localhost/mydb?retries=off            |

### Custom Driver Options

The driver package exposes the function `SetDSNOptions`, allowing for modification of the
connection by adding custom driver options.
It requires a full import of the driver (not by side-effects only).

Example of defining a custom option:

```golang
import (
 "database/sql"

 "github.com/go-mysql-org/go-mysql/driver"
)

func main() {
 driver.SetDSNOptions(map[string]DriverOption{
  "no_metadata": func(c *client.Conn, value string) error {
   c.SetCapability(mysql.CLIENT_OPTIONAL_RESULTSET_METADATA)
   return nil
  },
 })

 // dsn format: "user:password@addr/dbname?"
 dsn := "root@127.0.0.1:3306/test?no_metadata=true"
 db, _ := sql.Open(dsn)
 db.Close()
}
```

### Custom Driver Name

A custom driver name can be set via build options: `-ldflags '-X "github.com/go-mysql-org/go-mysql/driver.driverName=gomysql"'`.

This can be useful when using [GORM](https://gorm.io/docs/connecting_to_the_database.html#Customize-Driver):

```go
import (
  _ "github.com/go-mysql-org/go-mysql/driver"
  "gorm.io/driver/mysql"
  "gorm.io/gorm"
)

db, err := gorm.Open(mysql.New(mysql.Config{
  DriverName: "gomysql",
  DSN: "gorm:gorm@127.0.0.1:3306/test",
}))
```

### Custom NamedValueChecker

Golang allows for custom handling of query arguments before they are passed to the driver
with the implementation of a [NamedValueChecker](https://pkg.go.dev/database/sql/driver#NamedValueChecker). By doing a full import of the driver (not by side-effects only),
a custom NamedValueChecker can be implemented.

```golang
import (
 "database/sql"

 "github.com/go-mysql-org/go-mysql/driver"
)

func main() {
 driver.AddNamedValueChecker(func(nv *sqlDriver.NamedValue) error {
  rv := reflect.ValueOf(nv.Value)
  if rv.Kind() != reflect.Uint64 {
   // fallback to the default value converter when the value is not a uint64
   return sqlDriver.ErrSkip
  }

  return nil
 })

 conn, err := sql.Open("mysql", "root@127.0.0.1:3306/test")
 defer conn.Close()

 stmt, err := conn.Prepare("select * from table where id = ?")
 defer stmt.Close()
 var val uint64 = math.MaxUint64
 // without the NamedValueChecker this query would fail
 result, err := stmt.Query(val)
}
```


We pass all tests in https://github.com/bradfitz/go-sql-test using go-mysql driver. :-)

## Logging

Logging uses [log/slog](https://pkg.go.dev/log/slog) and by default is sent to standard out.

For the old logging package `github.com/siddontang/go-log/log`, a converting package
`https://github.com/serprex/slog-siddontang` is available.
## How to migrate to this repo
To change the used package in your repo it's enough to add this `replace` directive to your `go.mod`:
```
replace github.com/siddontang/go-mysql => github.com/go-mysql-org/go-mysql v1.13.0
```

This can be done by running this command:
```
go mod edit -replace=github.com/siddontang/go-mysql=github.com/go-mysql-org/go-mysql@v1.13.0
```

v1.13.0 - is the last tag in repo, feel free to choose what you want.

## Credits

go-mysql was started by @siddontang and has many [contributors](https://github.com/go-mysql-org/go-mysql/graphs/contributors)
