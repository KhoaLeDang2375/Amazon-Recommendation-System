FROM apache/hadoop:3.5.0
USER root
ARG FLUME_VERSION=1.11.0
RUN curl -fsSL "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" -o /tmp/flume.tar.gz \
    && curl -fsSL "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz.sha512" -o /tmp/flume.sha512 \
    && expected="$(grep -Eo '[[:xdigit:]]{128}' /tmp/flume.sha512 | head -n 1)" \
    && test -n "$expected" \
    && echo "$expected  /tmp/flume.tar.gz" | sha512sum -c - \
    && mkdir -p /opt/flume \
    && tar -xzf /tmp/flume.tar.gz --strip-components=1 -C /opt/flume \
    && rm /tmp/flume.tar.gz /tmp/flume.sha512 \
    && chown -R 1001:100 /opt/flume
COPY flume-env.sh /opt/flume/conf/flume-env.sh
COPY log4j2.xml /opt/flume/conf/log4j2.xml
USER hadoop
CMD ["/opt/flume/bin/flume-ng", "agent", "--conf", "/opt/flume/conf", "--conf-file", "/opt/flume/conf/ingestion.conf", "--name", "ingestion", "-Dflume.root.logger=INFO,console"]
