---
layout: page
title: "Base tutorial"
category: getting
date: 2017-08-21 12:49:55
---

On this page, we can to try a stream example using a real Kafka cluster and the cep jar artifact. We are going to suppose that you have built the cep distribution how we explain on the [Building](https://github.com/wizzie-io/zz-cep#compiling-sources) section.

### Explication
First of all, we need define a the config for launch a cep application.

#### CEP Config Json (config.json)

```json
{
  "application.id": "cep-instance-id",
  "bootstrap.servers": "localhost:9092",
  "num.stream.threads": 1,
  "bootstraper.classname": "io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper",
  "bootstrap.kafka.topics": ["__cep__bootstrapper"],
  "metric.enable": true,
  "metric.listeners": ["io.wizzie.metrics.ConsoleMetricListener"],
  "metric.interval": 60000
}
```

You can now start the cep application:

```
./cep-start.sh config/config.json
```

At this point the CEP application is listening for new stream processing definitions. You can send one with the "streamer-kafka.sh" tool.
As an example:

#### Stream processing definition
```json
{
    "streams": [
        {
            "streamName": "streaminput",
            "attributes": [
                {
                    "name": "timestamp",
                    "type": "long"
                }
            ]
        }
    ],
    "rules": [
        {
            "id": "1",
            "version": "v1",
            "streams": {
                "in": [
                    {
                        "streamName": "streaminput",
                        "kafkaTopic": "kafkainput"
                    }
                ],
                "out": [
                    {
                        "streamName": "streamoutput",
                        "kafkaTopic": "kafkaoutput"
                    }
                ]
            },
            "executionPlan": "from streaminput select * insert into streamoutput"
        },
        {
            "id": "2",
            "version": "v1",
            "streams": {
                "in": [
                    {
                        "streamName": "streaminput",
                        "kafkaTopic": "kafkainput"
                    }
                ],
                "out": [
                    {
                        "streamName": "streamoutput",
                        "kafkaTopic": "kafkaoutput"
                    }
                ]
            },
            "executionPlan": "from streaminput select * insert into streamoutput"
        }
    ]
}
```

```
./bin/streamer-kafka.sh localhost:9092 cep-instance-id config/sample_processing_model.json
```

On this example we read the Kafka topic `kafkainput`, map it to the stream `streaminput` and insert every field to the stream `streamoutput` which is mapped to the Kafka topic `kafkaoutput`

Now we can send an event to kafka topic `kafkainput`.

```json
{"timestamp":123456789}
```

The CEP instance should output the same at kafka topic `kafkaoutput`:

```json
{"timestamp":123456789}
```

### Execution

On the first time we need to have a running Kafka cluster and the decompressed cep distribution.

#### Config file

We need to modify the config file that is inside the folder `config/sample_config.json`, we can change it or destroy it and create a new one with this content.

```json
{
  "application.id": "cep-instance-id",
  "bootstrap.servers": "localhost:9092",
  "num.stream.threads": 1,
  "bootstraper.classname": "io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper",
  "bootstrap.kafka.topics": ["__cep_bootstrapper"],
  "metric.enable": true,
  "metric.listeners": ["io.wizzie.ks.cep.metrics.ConsoleMetricListener"],
  "metric.interval": 60000
}
```

On this config file we indicate the `application.id` that will identify our instances group and the running Zookeeper and some Kafka Broker. On the example we are going to use the `KafkaBootstrapper` so we read the config using the kafka cluster.

Now we can start the cep service to do that we can uses the init script that is inside the folder bin:

```
cep/bin/cep-start.sh cep/config/sample_config.json
```

Now you can produce some input message into `kafkainput` Kafka topic, but first you could open a Kafka consumer to check the output messages.

* Consumer
```
kafka_dist/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --property print.key=true --topic kafkaoutput --new-consumer
```

* Producer
```
kafka_dist/bin/kafka-console-producer.sh --broker-list localhost:9092 --property parse.key=true --property key.separator=, --topic kafkainput
```

You can write some message into console-producer:

```json
null,{"timestamp":123456789}
```

and you must see the output message on the console-consumer:

```json
null,{"timestamp":123456789}
```

This is the end of the tutorial!! Congratulations! ;)


