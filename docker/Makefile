all: build

build:
	@docker build -t go-mysql .
	@mkdir -p /tmp/go-mysql/1
	@mkdir -p /tmp/go-mysql/2
	@mkdir -p /tmp/go-mysql/3
	@mkdir -p /tmp/go-mysql/4
	@mkdir -p /tmp/go-mysql/5
	@mkdir -p /tmp/go-mysql/6
	@docker run -d -p 3306:3306 -v /tmp/go-mysql/1:/var/lib/mysql --name=mysql1 -e "GTID_MODE=off" -e "SERVER_ID=1" go-mysql
	@docker run -d -p 3307:3306 -v /tmp/go-mysql/2:/var/lib/mysql --name=mysql2 -e "GTID_MODE=off" -e "SERVER_ID=2" go-mysql
	@docker run -d -p 3308:3306 -v /tmp/go-mysql/3:/var/lib/mysql --name=mysql3 -e "GTID_MODE=off" -e "SERVER_ID=3" go-mysql
	@docker run -d -p 3316:3306 -v /tmp/go-mysql/4:/var/lib/mysql --name=mysql4 -e "GTID_MODE=on" -e "SERVER_ID=4" go-mysql
	@docker run -d -p 3317:3306 -v /tmp/go-mysql/5:/var/lib/mysql --name=mysql5 -e "GTID_MODE=on" -e "SERVER_ID=5" go-mysql
	@docker run -d -p 3318:3306 -v /tmp/go-mysql/6:/var/lib/mysql --name=mysql6 -e "GTID_MODE=on" -e "SERVER_ID=6" go-mysql

clean:
	@docker rm -f mysql1 mysql2 mysql3 mysql4 mysql5 mysql6