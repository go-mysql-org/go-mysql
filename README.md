# go-mysql

A pure go library to handle MySQL client <-> server protocol.

Forked from [mixer](https://github.com/siddontang/mixer) mysql module, with some changes.

## Client

Client package supports a simple MySQL connection driver which you can use it to communicate with MySQL server. 

## Server

Server package supplies a framework to implement a simple MySQL server which can handle the packets from the MySQL client. 
You can use it to build your own MySQL proxy. 

## Replication

Replication package handles mysql replication protocol like [python-mysql-replication](https://github.com/noplay/python-mysql-replication).

You can use it acting like a MySQL slave to sync binlog from master then do somethings, like updating cache, etc...

## Feedback

go-mysql is still in development, your feedback is very welcome. 


Gmail: siddontang@gmail.com