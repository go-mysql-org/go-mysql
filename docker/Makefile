IMAGE=siddontang/mysql:latest

all: build

build:
	@docker run -d -p 3306:3306 --name=mysql1 -e "GTID_MODE=on" -e "SERVER_ID=1" ${IMAGE}
	@docker run -d -p 3307:3306 --name=mysql2 -e "GTID_MODE=on" -e "SERVER_ID=2" ${IMAGE}
	@docker run -d -p 3308:3306 --name=mysql3 -e "GTID_MODE=on" -e "SERVER_ID=3" ${IMAGE}

image:
	@docker pull ${IMAGE}

stop:
	@docker stop mysql1 mysql2 mysql3 

clean:
	@docker rm -f mysql1 mysql2 mysql3