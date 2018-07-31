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
ENV BOOTSTRAP_KAFKA_TOPICS [\"__cep_bootstrap\"]

ENV LOG_LEVEL info
ENV GENERAL_LOG_LEVEL info

ENV JVM_OPTIONS -Xmx512m -Xms512m

CMD exec /bin/cep-start.sh
