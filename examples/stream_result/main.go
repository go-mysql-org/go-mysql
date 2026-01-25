// This example demonstrates how to use StreamResult to send query results
// row by row in a streaming fashion. This is useful for large result sets
// that don't fit in memory or need to be sent incrementally.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// StreamResultHandler implements the server.Handler interface
// and demonstrates streaming query results.
type StreamResultHandler struct{}

func (h *StreamResultHandler) UseDB(dbName string) error {
	log.Printf("USE %s", dbName)
	return nil
}

func (h *StreamResultHandler) HandleQuery(query string) (*mysql.Result, error) {
	log.Printf("Query: %s", query)

	// Example: Stream a large result set for "SELECT * FROM users"
	if query == "SELECT * FROM users" {
		return h.handleStreamingQuery()
	}

	// For other queries, return a simple result
	return nil, nil
}

func (h *StreamResultHandler) handleStreamingQuery() (*mysql.Result, error) {
	// Define the result columns
	fields := []*mysql.Field{
		{Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONG},
		{Name: []byte("name"), Type: mysql.MYSQL_TYPE_VAR_STRING},
		{Name: []byte("email"), Type: mysql.MYSQL_TYPE_VAR_STRING},
	}

	// Create a StreamResult with buffer size of 10
	sr := mysql.NewStreamResult(fields, 10, true)

	// Start a goroutine to produce rows
	go func() {
		defer sr.Close()
		ctx := context.Background()

		// Simulate streaming 100 rows
		for i := 1; i <= 100; i++ {
			row := []any{
				i,
				fmt.Sprintf("user_%d", i),
				fmt.Sprintf("user%d@example.com", i),
			}

			// WriteRow returns false if the stream is closed or context is canceled
			if !sr.WriteRow(ctx, row) {
				log.Printf("Stream closed, stopping at row %d", i)
				return
			}

			// Simulate some processing delay
			time.Sleep(10 * time.Millisecond)
		}
		log.Printf("Finished streaming all rows")
	}()

	// Return the StreamResult wrapped as a Result
	return sr.AsResult(), nil
}

func (h *StreamResultHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

func (h *StreamResultHandler) HandleStmtPrepare(query string) (params int, columns int, ctx interface{}, err error) {
	return 0, 0, nil, nil
}

func (h *StreamResultHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, nil
}

func (h *StreamResultHandler) HandleStmtClose(ctx interface{}) error {
	return nil
}

func (h *StreamResultHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return nil
}

func main() {
	// Create a MySQL server with default settings
	srv := server.NewDefaultServer()

	// Listen on TCP port 4000
	listener, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	log.Println("MySQL server listening on 127.0.0.1:4000")
	log.Println("Connect with: mysql -h 127.0.0.1 -P 4000 -u root")
	log.Println("Try: SELECT * FROM users")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go handleConnection(srv, conn)
	}
}

func handleConnection(srv *server.Server, conn net.Conn) {
	defer conn.Close()

	handler := &StreamResultHandler{}

	// Create a new MySQL connection using the server instance
	mysqlConn, err := srv.NewConn(conn, "root", "", handler)
	if err != nil {
		log.Printf("Failed to create MySQL connection: %v", err)
		return
	}
	log.Printf("Client connected: %s", conn.RemoteAddr())

	// Handle commands in a loop
	for {
		if err := mysqlConn.HandleCommand(); err != nil {
			log.Printf("Connection closed: %v", err)
			return
		}
	}
}
