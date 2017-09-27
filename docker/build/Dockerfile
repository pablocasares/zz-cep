FROM java:openjdk-8-jre

MAINTAINER agomez@wizzie.io

RUN apt-get update
RUN apt-get install -y gettext-base

RUN mkdir -p /var/log/zz-cep

ADD zz-cep-dist.tar.gz /opt/
RUN rm -f /opt/zz-cep/config/*

ADD log4j.xml /opt/zz-cep/config/
ADD config_env.json /opt/zz-cep/config/
ADD cep-start.sh /bin

ENV METRIC_ENABLE true
ENV METRIC_INTERVAL 60000
ENV NUM_STREAM_THREADS 1
ENV MULTI_ID false

ENV JVM_OPTIONS -Xmx512m -Xms512m

CMD /bin/cep-start.sh
