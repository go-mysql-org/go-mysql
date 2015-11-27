package main

import (
	"github.com/siddontang/go-mysql/server"
	"net"
	"fmt"
)

func main() {
	l, _ := net.Listen("tcp","127.0.0.1:4000")


	for {
		c, _ := l.Accept()
		fmt.Println("accepted")

		go func() {
			fmt.Println("goroutine")
			// Create a connection with user root and an empty passowrd
			// We only an empty handler to handle command too
			conn, _ := server.NewConn(c, "u", "p", server.EmptyHandler{})
			fmt.Println("conn created")
			for {
				if err :=	conn.HandleCommand() ; err != nil {
					fmt.Println(fmt.Sprintf("error: %+v", err))
					return
				}
				fmt.Println("command handled")
				if conn.Conn == nil {
					fmt.Println("detected command closed")
					return
				}
			}
			fmt.Println("command handled2")
		}()
	}
}
