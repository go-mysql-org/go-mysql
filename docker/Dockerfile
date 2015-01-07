FROM ubuntu:14.04
MAINTAINER siddontang@gmail.com

# Use Chinese sources.list to speed up if you are in China
ADD sources.list /etc/apt/sources.list

RUN apt-get update \
    && apt-get install -y mysql-server-5.6 openssh-server

RUN mkdir /var/run/sshd
RUN echo 'root:123123' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

ADD my.cnf /etc/mysql/my.cnf
ADD my-default.cnf /usr/share/mysql/my-default.cnf

ADD start.sh /start.sh
RUN chmod +x /start.sh


COPY init.sh /init.sh
RUN chmod +x /init.sh

# VOLUME ["/var/lib/mysql"]

EXPOSE 3306
EXPOSE 22

RUN /init.sh

CMD ["/start.sh"]