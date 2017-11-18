FROM tarantool/tarantool:1.5

COPY tarantool.cfg /usr/local/etc/tarantool.cfg
WORKDIR /data/tarantool/
RUN chown -R tarantool:tarantool /data/tarantool/ \
    && su-exec tarantool tarantool_box --config /usr/local/etc/tarantool.cfg --init-storage

EXPOSE 2001
ENTRYPOINT ["su-exec", "tarantool", "tarantool_box", "--config", "/usr/local/etc/tarantool.cfg"]
