package driver

import (
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// DriverOption sets configuration on a client connection before the MySQL handshake.
// The value represents the query string parameter value supplied by in the DNS.
type DriverOption func(c *client.Conn, value string) error

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
	var (
		b   bool
		err error
	)
	if b, err = strconv.ParseBool(value); err != nil {
		return errors.Errorf("invalid boolean value '%s' for compress option", value)
	}
	if b {
		c.SetCapability(mysql.CLIENT_COMPRESS)
	}

	return nil
}
