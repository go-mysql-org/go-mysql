package client_test

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func Example() {
	// Connect MySQL at 127.0.0.1:3306, with user root, an empty password and database test
	conn, err := client.Connect("127.0.0.1:3306", "root", "", "test")
	// Or to use SSL/TLS connection if MySQL server supports TLS
	// conn, err := client.Connect("127.0.0.1:3306", "root", "", "test", func(c *Conn) {c.UseSSL(true)})
	// Or to set your own client-side certificates for identity verification for security
	// tlsConfig := NewClientTLSConfig(caPem, certPem, keyPem, false, "your-server-name")
	// conn, err := client.Connect("127.0.0.1:3306", "root", "", "test", func(c *Conn) {c.SetTLSConfig(tlsConfig)})
	if err != nil {
		msg := fmt.Sprintf(`
This example needs a MySQL listening on 127.0.0.1:3006 with user "root" and 
empty password. Please check the connectivity using mysql client.
---
Connect to MySQL failed: %v`, err)
		panic(msg)
	}

	err = conn.Ping()
	if err != nil {
		panic(err)
	}

	// (re)create the t1 table
	r, err := conn.Execute(`DROP TABLE IF EXISTS t1`)
	if err != nil {
		panic(err)
	}
	r.Close()
	r, err = conn.Execute(`CREATE TABLE t1 (id int PRIMARY KEY, name varchar(255))`)
	if err != nil {
		panic(err)
	}
	r.Close()

	// Insert
	r, err = conn.Execute(`INSERT INTO t1(id, name) VALUES(1, "abc"),(2, "def")`)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	// Get last insert id and number of affected rows
	fmt.Printf("InsertId: %d, AffectedRows: %d\n", r.InsertId, r.AffectedRows)

	// Select
	r, err = conn.Execute(`SELECT id, name FROM t1`)
	if err != nil {
		panic(err)
	}

	// Handle resultset
	v, err := r.GetInt(0, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Value of Row 0, Column 0: %d\n", v)

	v, err = r.GetIntByName(0, "id")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Value of Row 0, Column 'id': %d\n", v)

	// Direct access to fields
	for rownum, row := range r.Values {
		fmt.Printf("Row number %d\n", rownum)
		for colnum, val := range row {
			fmt.Printf("\tColumn number %d\n", colnum)

			ival := val.Value() // interface{}
			fmt.Printf("\t\tvalue (type: %d): %#v\n", val.Type, ival)

			if val.Type == mysql.FieldValueTypeSigned {
				fval := val.AsInt64()
				fmt.Printf("\t\tint64 value: %d\n", fval)
			}
			if val.Type == mysql.FieldValueTypeString {
				fval := val.AsString()
				fmt.Printf("\t\tstring value: %s\n", fval)
			}
		}
	}
	// Output:
	// InsertId: 0, AffectedRows: 2
	// Value of Row 0, Column 0: 1
	// Value of Row 0, Column 'id': 1
	// Row number 0
	// 	Column number 0
	// 		value (type: 2): 1
	// 		int64 value: 1
	// 	Column number 1
	// 		value (type: 4): []byte{0x61, 0x62, 0x63}
	// 		string value: abc
	// Row number 1
	// 	Column number 0
	// 		value (type: 2): 2
	// 		int64 value: 2
	// 	Column number 1
	// 		value (type: 4): []byte{0x64, 0x65, 0x66}
	// 		string value: def
}
