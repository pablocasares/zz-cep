---
title: Metrics
layout: single
toc: true
---

The ZZ-CEP service uses the [Wizzie Metrics Library](https://github.com/wizzie-io/metrics-library) to build his metrics, by default it sends JVM metrics but you can register new metrics that will be exported too.

## Properties

The metrics service has four properties to configure it:

| Property   |      Description      |  Default Value |
|----------|---------------|-------|
| `metric.enable` |  Enable or disable metrics service | false|
| `metric.listeners` | The listener to send the metrics. [Available listeners](https://wizzie-io.github.io/zz-cep/configuration/metrics)  |   ["io.wizzie.cep.metrics.ConsoleMetricListener"] |
| `metric.interval` | The interval time to report metrics (milliseconds) |  60000  |
| `metric.verbose.mode`| Enable the verbose metric mode | false |

The listeners are the process that listen the reported metrics and do something with them. You can have multiple listeners at the same time. Currently, the ZZ-CEP service supports two listener metrics:

## Listeners

### ConsoleMetricListener

This listener `io.wizzie.metrics.ConsoleMetricListener` sends the transform the metrics to JSON and prints them into the log file using the log4j. The metric format is:

```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345}
```

### KafkaMetricListener

This listener `io.wizzie.metrics.KafkaMetricListener` send the transform the metrics to JSON and sends them into the Kafka topic. The metric format is:
```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345, "app_id":"MY_KAFKA_STREAMS_APP_ID"}
```

The `metric.kafka.topic` used by default is `__cep_metrics`

### Custom Listeners
You can made new listeners to do this you need to implement the [MetricListener Class](https://github.com/wizzie-io/metrics-library/blob/master/src/main/java/io/wizzie/metrics/listeners/MetricListener.java).
