MYSQL_IMAGE=siddontang/mysql:latest
MARIADB_IMAGE=siddontang/mariadb:latest

all: build

build:
	@docker run -d -p 3306:3306 --name=mysql1 -e "GTID_MODE=on" -e "SERVER_ID=1" ${MYSQL_IMAGE}
	@docker run -d -p 3307:3306 --name=mysql2 -e "GTID_MODE=on" -e "SERVER_ID=2" ${MYSQL_IMAGE}
	@docker run -d -p 3308:3306 --name=mysql3 -e "GTID_MODE=on" -e "SERVER_ID=3" ${MYSQL_IMAGE}
	@docker run -d -p 3316:3306 --name=mariadb1 -e "SERVER_ID=4" ${MARIADB_IMAGE}
	@docker run -d -p 3317:3306 --name=mariadb2 -e "SERVER_ID=5" ${MARIADB_IMAGE}
	@docker run -d -p 3318:3306 --name=mariadb3 -e "SERVER_ID=6" ${MARIADB_IMAGE}

image:
    @docker pull ${MYSQL_IMAGE}
    @docker pull ${MARIADB_IMAGE}

stop:
    @docker stop mysql1 mysql2 mysql3 mariadb4 mariadb5 mariadb6

clean:
    @docker rm -f mysql1 mysql2 mysql3 mariadb4 mariadb5 mariadb6
