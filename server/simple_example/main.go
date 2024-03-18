package main

import (
	"net"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/siddontang/go-log/log"
)

func main() {
	l, _ := net.Listen("tcp", "127.0.0.1:4306")
	provider := server.NewInMemoryProvider()
	provider.AddUser("root", "root")
	// var tlsConf = server.NewServerTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, tls.VerifyClientCertIfGiven)
	for {
		c, _ := l.Accept()
		go func() {
			// Create a connection with user root and an empty password.
			// You can use your own handler to handle command here.
			svr := server.NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, nil, nil)
			conn, err := server.NewCustomizedConn(c, svr, provider, server.EmptyHandler{})

			if err != nil {
				log.Errorf("Connection error: %v", err)
				return
			}

			err = conn.RunSimpleCopy()
			if err != nil {
				log.Errorf(`simple proxy error: %v`, err)
				return
			}
		}()
	}
}
