---
title: Boostrapper Configuration
layout: single
toc: true
---

The bootstrapper is the mechanism that is used to load stream config into cep. You can configure the boostrapper class on the config file, using `bootstrapper.classname` property. Currently, the cep has two bootstrappers:

## Bootstrappers

### Kafka Bootstrapper

`io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper`

This bootstrapper read the stream config from Kafka, so you can change the stream topology without restart the service. The bootstrapper is reading the topic `__cep_bootstrap` using a kafka consumer instance with a random `group.id`.

| Property     | Description     |
| :------------- | :-------------  |
| `bootstrap.kafka.topics`      | Topics that are used to read the bootstrapper configuration      |
| `application.id`      | The app id to identify the client configuration      |
| `bootstrap.servers`      | The kafka broker to read the bootstrapper configuration      |

### StreamerKafkaConfig

The StreamerKafkaConfig tool allow us to send new stream config to the cep and read the current stream config.

You can use the script [streamer-kafka.sh](https://github.com/wizzie-io/zz-cep/blob/master/bin/streamer-kafka.sh) to use this tool. The tool has two modes:

#### Read Mode

The read mode allow us to read the current stream configuration to specific cep instance.

```bash
bin/streamer-kafka.sh $BOOTSTRAP_KAFKA_SERVER $APPLICATION_ID
```

#### Write Mode

The write mode allow us to send new stream configuration to specific cep instance.

```bash
bin/streamer-kafka.sh $BOOTSTRAP_KAFKA_SERVER $APPLICATION_ID $STREAM_CONFIG_FILE
```


Library:  [Bootstrapper](https://github.com/wizzie-io/config-bootstrapper)

### File Bootstrapper

`io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper`

This bootstrapper read the stream config from local file system, and build a KS topology using this file. You need to add the properties on the configuration file.

| Property     | Description     |
| :------------- | :-------------  |
| `file.bootstrapper.path`      | Stream config file path      |

Library:  [Bootstrapper](https://github.com/wizzie-io/config-bootstrapper)