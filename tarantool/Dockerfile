FROM centos:centos7

RUN yum install scl-utils -y
RUN rpm -i http://tarantool.org/dist/master/centos/7/os/x86_64/Packages/mailru-15-runtime-1.0-1.noarch.rpm http://tarantool.org/dist/master/centos/7/os/x86_64/Packages/mailru-15-tarantool-1.5.5-9.x86_64.rpm

ADD tarantool.cfg /usr/local/etc/tarantool.cfg

RUN /opt/tarantool/mailru-15/root/bin/tarantool_box --config /usr/local/etc/tarantool.cfg --init-storage

EXPOSE 2001

ENTRYPOINT ["/opt/tarantool/mailru-15/root/bin/tarantool_box", "--config", "/usr/local/etc/tarantool.cfg"]
