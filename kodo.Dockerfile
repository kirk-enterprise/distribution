FROM index-dev.qiniu.io/kelibrary/ke-ubuntu-1604:20180328

RUN apt-get update && apt-get install -y \
    apache2-utils \
 && rm -rf /var/lib/apt/lists/*

COPY bin/registry /bin/registry
COPY cmd/registry/config-dev.yml /etc/docker/registry/config.yml

VOLUME ["/var/lib/registry"]
EXPOSE 5000
ENTRYPOINT ["registry"]
CMD ["serve", "/etc/docker/registry/config.yml"]
