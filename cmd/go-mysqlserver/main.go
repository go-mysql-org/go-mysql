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

	log.Println("Listening on port 4000, connect with 'mysql -h 127.0.0.1 -P 4000 -u root'")

	// Accept a new connection once
	c, err := l.Accept()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Accepted connection")

	// Create a connection with user root and an empty password.
	// You can use your own handler to handle command here.
	conn, err := server.NewConn(c, "root", "", server.EmptyHandler{})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Registered the connection with the server")

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			log.Fatal(err)
		}
	}
}
