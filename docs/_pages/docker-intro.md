---
layout: page
title: "Docker Intro"
category: docker
date: 2017-08-17 10:57:26
---

## How to get CEP image
You can pull the docker image from our registry:
```
$ docker pull gcr.io/wizzie-registry/cep:0.1.0
```

## How to use this image

Cep requires a running Kafka broker before it starts.
### Link Cep to Kafka broker container

#### Zookeeper

First, start a zookeeper container by executing:

```
$ docker run --rm --name zookeeper-svc --net=host wurstmeister/zookeeper
```

You can found more information about `wurstmeister/zookeeper` image [here](https://hub.docker.com/r/wurstmeister/zookeeper)

#### Kafka
Now, start a kafka broker container by executing:

```
$ docker run --rm --name kafka-broker --net=host -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 -e KAFKA_ADVERTISED_PORT=9092 wurstmeister/kafka:0.10.2.1
```
You can found more information about `wurstmeister/kafka` image [here](https://hub.docker.com/r/wurstmeister/kafka)

#### Start Cep

Once kafka broker is up, we can start a Cep container and link it to the kafka broker, and configuring the APPLICATION_ID environment variable with our custom app name:

```
$ docker run --rm --name my-cep --net=host -e APPLICATION_ID=my-cep-app -e KAFKA_BOOTSTRAP_SERVER=localhost:9092 gcr.io/wizzie-registry/cep:0.1.0
```
Now you can follow the [base tutorial](http://www.wizzie.io/cep/getting/getting-started.html) to test Cep!

### Using environment variables in cep configuration

You can configure the docker image using these environment properties:

| Env Property   |      Description      |  Default Value |
|----------|---------------|-------|
| `APPLICATION_ID` |  This id is used to identify a group of cep instances | null |
| `KAFKA_BOOTSTRAP_SERVER` |  Kafka servers list | null |
| `NUM_STREAM_THREADS` |  Number parallelism | 1|
| `METRIC_ENABLE` | Enable the metrics |  true  |
| `METRIC_INTERVAL`|The interval time to report metrics (milliseconds) | 60000 |

You can found more information about base configuration [here](http://www.wizzie.io/cep/conf/base-configuration.html)
