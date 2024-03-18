package server

import (
	"fmt"
	"io"
	"net"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/siddontang/go-log/log"
)

const (
	BackendAddr   string = "backend_addr"
	BackendUser   string = "backend_user"
	BackendPasswd string = "backend_passwd"
)

type NetConnGetter interface {
	GetConnection() net.Conn
}

// RunSimpleCopy blocks until the front/back connection is close.
func (c *Conn) RunSimpleCopy() (err error) {
	if c.Conn == nil {
		return fmt.Errorf("connection closed")
	}

	defer func() {
		c.writeError(err)
		c.Conn.Close()
	}()

	// get backend connection info
	var (
		addr, user, passwd string
		backConn           *client.Conn
	)

	addr = c.attributes[BackendAddr]
	user = c.attributes[BackendUser]
	passwd = c.attributes[BackendPasswd]

	if addr == "" || user == "" {
		err = fmt.Errorf("connection attributes is invalid: %+v", c.attributes)
		return err
	}

	log.Infof("client %s try to connect backend(%s, %s, %s)", c.RemoteAddr().String(), addr, user, passwd)

	backConn, err = client.Connect(addr, user, passwd, c.db)
	if err != nil {
		return err
	}
	defer backConn.Close()

	// connect 2 connections

	var errChan = make(chan error)
	cpFunc := func(dst io.Writer, src io.Reader) {
		_, err = io.Copy(dst, src)
		errChan <- err
	}
	go cpFunc(c.Conn, backConn)
	go cpFunc(backConn, c.Conn)
	err = <-errChan
	if err != nil {
		err = fmt.Errorf("unexpected disconnection: %+v", err)
		return err
	}

	log.Info("connection is closed on purpose")

	return nil
}
