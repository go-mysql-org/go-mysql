#!/bin/bash 

set -e

GTID_MODE=${GTID_MODE:-off}
SERVER_ID=${SERVER_ID:-0}

chown -R mysql:mysql /var/lib/mysql

/usr/sbin/sshd 
/usr/bin/mysqld_safe --gtid_mode=${GTID_MODE} --server-id=${SERVER_ID} 