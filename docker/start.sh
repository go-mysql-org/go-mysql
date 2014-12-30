#!/bin/bash 

set -e

GTID_MODE=${GTID_MODE:-off}
SERVER_ID=${SERVER_ID:-0}

mkdir -p -m 700 /var/lib/mysql
chown -R mysql:mysql /var/lib/mysql

#
# the default password for the debian-sys-maint user is randomly generated
# during the installation of the mysql-server package.
#
# Due to the nature of docker we blank out the password such that the maintenance
# user can login without a password.
#
sed 's/password = .*/password = /g' -i /etc/mysql/debian.cnf

# initialize MySQL data directory
if [ ! -d /var/lib/mysql/ibdata1 ]; then
  echo "Installing database..."
  mysql_install_db --user=mysql >/dev/null 2>&1

  # start mysql server
  echo "Starting MySQL server..."
  /usr/bin/mysqld_safe >/dev/null 2>&1 &

  # wait for mysql server to start (max 30 seconds)
  timeout=30
  while ! /usr/bin/mysqladmin -u root status >/dev/null 2>&1
  do
    timeout=$(($timeout - 1))
    if [ $timeout -eq 0 ]; then
      echo "Could not connect to mysql server. Aborting..."
      exit 1
    fi
    echo "Waiting for database server to accept connections..."
    sleep 1
  done

  ## create a localhost only, debian-sys-maint user
  ## the debian-sys-maint is used while creating users and database
  ## as well as to shut down or starting up the mysql server via mysqladmin
  echo "Creating debian-sys-maint user..."
  mysql -uroot -e "GRANT ALL PRIVILEGES on *.* TO 'debian-sys-maint'@'localhost' IDENTIFIED BY '' WITH GRANT OPTION;"

  echo "Grant ALL to root"
  mysql -uroot -e "GRANT ALL ON *.* TO 'root'@'%' IDENTIFIED BY '' WITH GRANT OPTION;"

  /usr/bin/mysqladmin --defaults-file=/etc/mysql/debian.cnf shutdown
fi

exec /usr/bin/mysqld_safe --gtid_mode=${GTID_MODE} --server-id=${SERVER_ID}