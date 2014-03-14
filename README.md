# go-mysql

go mysql client interface

a fork from [mixer](https://github.com/siddontang/mixer) mysql module.

go-mysql provieds a simple interface for mysql use, very like golang database/sql but has a little different.

    //create a db, with max 16 idle connections
    //dsn format is <username>:<password>@<host>:<port>/<database>
    db := NewDB("qing:admin@127.0.0.1:3306/mixer", 16)

    //ping remote mysql server is alive
    db.Ping()

    //exec a query, return Result
    r, err := db.Exec("insert into mixer_conn (id, str) values (1, `abc`)")
    println(r.LastInsertId(), r.RowsAffected())

    //exec a query with placeholds, return Result
    r, err := db.Exec("insert into mixer_conn (id, str) values (?, ?)", 2, "efg")
    println(r.LastInsertId(), r.RowsAffected())

    //query a query, return Resultset
    r, err := db.Query("select str from mixer_conn where id = 1")
    str, _ = r.GetString(0, 0)
    str, _ = r.GetStringByName(0, "str")

    //begin a transaction
    tx, err = db.Begin()

    //tx exec a query
    tx.Exec("insert into mixer_conn (id, str) values (3, `abc`)")

    //tx commit
    tx.Commit()

    //prepare statement
    s, err := db.Prepare("insert into mixer_conn (id, str) values(?, ?)")
    
    //exec statement
    s.Exec(5, "abc")
    
    //close statement
    s.Close()

    //get a conn for special use, like set charset
    conn, err = db.GetConn()

    conn.SetCharset("gb2312")
    
# golang database/sql

go-mysql now can be use with golang database/sql like other sql drivers.

I test it using testcase in [https://github.com/bradfitz/go-sql-test](https://github.com/bradfitz/go-sql-test)
