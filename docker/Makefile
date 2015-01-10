IMAGE=siddontang/mysql:latest

all: build

build:
	@docker run -d -p 3306:3306 --name=mysql1 -e "GTID_MODE=off" -e "SERVER_ID=1" ${IMAGE}
	@docker run -d -p 3307:3306 --name=mysql2 -e "GTID_MODE=off" -e "SERVER_ID=2" ${IMAGE}
	@docker run -d -p 3308:3306 --name=mysql3 -e "GTID_MODE=off" -e "SERVER_ID=3" ${IMAGE}
	@docker run -d -p 3316:3306 --name=mysql4 -e "GTID_MODE=on" -e "SERVER_ID=4" ${IMAGE}
	@docker run -d -p 3317:3306 --name=mysql5 -e "GTID_MODE=on" -e "SERVER_ID=5" ${IMAGE}
	@docker run -d -p 3318:3306 --name=mysql6 -e "GTID_MODE=on" -e "SERVER_ID=6" ${IMAGE}

image:
	@docker pull ${IMAGE}

stop:
	@docker stop mysql1 mysql2 mysql3 mysql4 mysql5 mysql6

clean:
	@docker rm -f mysql1 mysql2 mysql3 mysql4 mysql5 mysql6