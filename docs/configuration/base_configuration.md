---
title: Base Configuration
layout: single
toc: false
---

The configuration file is a JSON format file where you specific the general properties to configure the cep instance. This file is different from the stream processing config file that defines the CEP processing rules.

Example configuration file:

```json
{
  "application.id": "cep-instance-id",
  "bootstrap.servers": "localhost:9092",
  "num.stream.threads": 1,
  "bootstrapper.classname": "io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper",
  "bootstrap.kafka.topics": ["__cep_bootstrapper"],
  "metric.enable": true,
  "metric.listeners": ["io.wizzie.cep.metrics.ConsoleMetricListener"],
  "metric.interval": 60000,
  "multi.id": false
}
```

| Property     | Description     |  Default Value|
| :------------- | :-------------  |   :-------------:   |
| `application.id`      | This id is used to identify a group of cep instances. Normally this id is used to identify different clients.      |  - |
| `bootstrap.servers`      | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form `host1:port1,host2:port2`      | - |
| `num.stream.threads`      | The number of threads to execute stream processing.      | 1 |
| `bootstrapper.classname`      | The bootstrapper class reference. More info: [Bootstrapper](http://wizzie-io.github.io/zz-cep/configuration/bootstrapper)       | - |
| `metric.enable`      | Enable metrics system.      | false |
| `metric.listeners`      | Array with metrics listeners. More info: [Metrics](http://wizzie-io.github.io/zz-cep/configuration/metrics)      | ["io.wizzie.cep.metrics.ConsoleMetricListener"] |
| `metric.interval`      | Metric report interval (milliseconds)      |  60000 |
| `metric.enable`      | Enable metrics system.      | false |
| `multi.id`      | This property is used when you have multiple cep instances with differences `application.id` and the cep uses the same topic names. More Info [Multi Tenant](https://github.com/wizzie-io/zz-cep/configuration/multi_tenant)      |  false |
