FROM ubuntu:14.04
MAINTAINER siddontang@gmail.com

# Use Chinese sources.list to speed up if you are in China
# ADD sources.list /etc/apt/sources.list

RUN apt-get update \
    && apt-get install -y mysql-server-5.6

RUN rm -rf /var/lib/mysql

ADD my.cnf /etc/mysql/my.cnf
ADD my-default.cnf /usr/share/mysql/my-default.cnf

ADD start.sh /start.sh
RUN chmod +x /start.sh

VOLUME ["/var/lib/mysql"]

EXPOSE 3306

CMD ["/start.sh"]