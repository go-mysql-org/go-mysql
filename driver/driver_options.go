package driver

import (
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// DriverOption sets configuration on a client connection before the MySQL handshake.
// The value represents the query string parameter value supplied by in the DNS.
type DriverOption func(c *client.Conn, value string) error

// UseSslOption sets the connection to use a tls.Config with InsecureSkipVerify set to true.
// Use SetTLSConfig() if you need a custom tls.Config
func UseSslOption(c *client.Conn) error {
	c.UseSSL(true)
	return nil
}

func CollationOption(c *client.Conn, value string) error {
	return c.SetCollation(value)
}

func ReadTimeoutOption(c *client.Conn, value string) error {
	var err error
	c.ReadTimeout, err = time.ParseDuration(value)
	return errors.Wrap(err, "invalid duration value for readTimeout option")
}

func WriteTimeoutOption(c *client.Conn, value string) error {
	var err error
	c.WriteTimeout, err = time.ParseDuration(value)
	return errors.Wrap(err, "invalid duration value for writeTimeout option")
}

func CompressOption(c *client.Conn, value string) error {
	switch value {
	case "zlib":
		c.SetCapability(mysql.CLIENT_COMPRESS)
	case "zstd":
		c.SetCapability(mysql.CLIENT_ZSTD_COMPRESSION_ALGORITHM)
	case "uncompressed":
		c.UnsetCapability(mysql.CLIENT_COMPRESS)
		c.UnsetCapability(mysql.CLIENT_ZSTD_COMPRESSION_ALGORITHM)
	default:
		return errors.Errorf("invalid compression algorithm '%s', valid values are 'zstd','zlib','uncompressed'", value)
	}

	return nil
}
