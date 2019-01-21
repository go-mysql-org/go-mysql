package schema

const (
	StorageType_Boltdb string = "boltdb"
	StorageType_Mysql  string = "mysql"
)

type TrackerConfig struct {
	// The charset_set_server of source mysql, we need
	// this charset to handle ddl statement
	CharsetServer string

	// Storage type to store schema data, may be boltdb or mysql
	Storage string

	// Boltdb file path to store data
	Dir string

	// MySQL info to connect
	Addr     string
	User     string
	Password string
	Database string
}
