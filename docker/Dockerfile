FROM ubuntu:14.04
MAINTAINER siddontang@gmail.com

RUN apt-get update \
    && apt-get install -y mysql-server-5.6

RUN rm -rf /var/lib/mysql
RUN rm -f /etc/mysql/my.cnf

ADD my.cnf /etc/mysql/my.cnf
RUN cp -f /etc/mysql/my.cnf /usr/share/mysql/my-default.cnf

ADD start.sh /start.sh
RUN chmod +x /start.sh

VOLUME ["/var/lib/mysql"]

EXPOSE 3306

CMD ["/start.sh"]