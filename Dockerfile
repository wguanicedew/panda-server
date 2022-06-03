FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel httpd httpd-devel gcc gridsite less git psmisc
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install -y postgresql14
RUN python3 -m venv /opt/panda
RUN /opt/panda/bin/pip install -U pip
RUN /opt/panda/bin/pip install -U setuptools
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-server.git#egg=panda-server[postgres]"
RUN /opt/panda/bin/pip install rucio-clients
RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-cacheschedconfig.git"
RUN ln -s /opt/panda/lib/python*/site-packages/mod_wsgi/server/mod_wsgi*.so /etc/httpd/modules/mod_wsgi.so

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/idds/idds.cfg.client.template /opt/panda/etc/idds/idds.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/panda/panda_server-httpd-FastCGI.conf.rpmnew /opt/panda/etc/panda/panda_server-httpd.conf

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
while true; do /opt/cacheschedconfig/bin/cacheSC.sh >> /var/log/panda/cacheSC.out; sleep 60; done & \n\
/etc/rc.d/init.d/httpd-pandasrv start \n ' > /etc/rc.d/init.d/run-panda-services

RUN chmod +x /etc/rc.d/init.d/run-panda-services

RUN ln -fs /opt/panda/etc/cert/hostkey.pem /etc/grid-security/hostkey.pem
RUN ln -fs /opt/panda/etc/cert/hostcert.pem /etc/grid-security/hostcert.pem
RUN ln -fs /opt/panda/etc/cert/chain.pem /etc/grid-security/chain.pem
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_server /etc/rc.d/init.d/httpd-pandasrv

RUN mkdir -p /data/atlpan
RUN mkdir -p /var/log/panda/wsgisocks
RUN mkdir -p /run/httpd/wsgisocks
RUN mkdir -p /var/cache/pandaserver
RUN chown -R atlpan:zp /var/log/panda

# to run with non-root PID
RUN chmod -R 777 /var/log/panda
RUN chmod -R 777 /run/httpd
RUN chmod -R 777 /home/atlpan
RUN chmod -R 777 /var/lock
RUN chmod -R 777 /var/cache/pandaserver

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

EXPOSE 25080 25443
