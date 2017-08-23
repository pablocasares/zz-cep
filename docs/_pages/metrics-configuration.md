---
layout: page
title: "Configuration"
category: metrics
date: 2017-02-14 13:21:42
order: 1
---

The Cep service uses the [Dropwizard Metrics](http://metrics.dropwizard.io/3.1.0/) to build his metrics, by default it sends JVM metrics but you can register new metrics that will be exported too.

The metrics service has four properties to configure it:

| Property   |      Description      |  Default Value |
|----------|---------------|-------|
| `metric.enable` |  Enable or disable metrics service | false|
| `metric.listeners` | The listener to send the metrics. [Available listeners](https://wizzie.io/cep/metrics/listeners.html)  |   ["io.wizzie.ks.cep.metrics.ConsoleMetricListener"] |
| `metric.interval` | The interval time to report metrics (milliseconds) |  60000  |
| `metric.verbose.mode`| Enable the verbose metric mode | false |

