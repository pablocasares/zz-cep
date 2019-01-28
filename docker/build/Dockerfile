FROM java:openjdk-8-jre

MAINTAINER devel@wizzie.io

RUN apt-get update
RUN apt-get install -y gettext-base

RUN mkdir -p /var/log/zz-cep

ADD zz-cep-dist.tar.gz /opt/
RUN rm -f /opt/zz-cep/config/*

ADD log4j2_env.xml /opt/zz-cep/config/
ADD config_env.json /opt/zz-cep/config/
ADD cep-start.sh /bin

ENV METRIC_ENABLE true
ENV METRIC_INTERVAL 60000
ENV METRIC_KAFKA_TOPIC __metrics
ENV MULTI_ID false
ENV BOOTSTRAP_CLASSNAME io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper
ENV BOOTSTRAP_KAFKA_TOPICS [\"__cep_bootstrap\"]

ENV LOG_LEVEL info
ENV GENERAL_LOG_LEVEL info

ENV JVM_OPTIONS="-Djdk.nio.maxCachedBufferSize=262144 -Xmx30m -Xms30m -XX:MaxDirectMemorySize=15m -XX:MaxMetaspaceSize=30m -XX:SurvivorRatio=6 -Xss512k -XX:ReservedCodeCacheSize=15m -XX:NewSize=15m " \
MALLOC_ARENA_MAX=1 \
PRODUCER_BUFFER_MEMORY=102400 \
CONSUMER_FETCH_MAX_BYTES=102400 \
CONSUMER_MAX_POLL_RECORDS=50 \
CONSUMER_FETCH_MAX_WAIT_MS=500 \
CONSUMER_SEND_BUFFER_BYTES=131072 \
CONSUMER_RECEIVE_BUFFER_BYTES=65536 \
PRODUCER_SEND_BUFFER_BYTES=131072 \
PRODUCER_RECEIVE_BUFFER_BYTES=32768

CMD exec /bin/cep-start.sh
