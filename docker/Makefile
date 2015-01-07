all: build

build:
	@docker build -t go-mysql .
	@docker run -d -p 8002:22 -p 3306:3306 --name=mysql1 -e "GTID_MODE=off" -e "SERVER_ID=1" go-mysql
	@docker run -d -p 8003:22 -p 3307:3306 --name=mysql2 -e "GTID_MODE=off" -e "SERVER_ID=2" go-mysql
	@docker run -d -p 8004:22 -p 3308:3306 --name=mysql3 -e "GTID_MODE=off" -e "SERVER_ID=3" go-mysql
	@docker run -d -p 8012:22 -p 3316:3306 --name=mysql4 -e "GTID_MODE=on" -e "SERVER_ID=4" go-mysql
	@docker run -d -p 8013:22 -p 3317:3306 --name=mysql5 -e "GTID_MODE=on" -e "SERVER_ID=5" go-mysql
	@docker run -d -p 8014:22 -p 3318:3306 --name=mysql6 -e "GTID_MODE=on" -e "SERVER_ID=6" go-mysql

clean:
	@docker rm -f mysql1 mysql2 mysql3 mysql4 mysql5 mysql6