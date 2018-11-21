---
title: Multitenant
layout: single
toc: false
---

The Cep service has a multi tenant mode, on this mode it prefix the `application.id` automatically on all the Kafka topics, except the bootstrapper and the metrics topic.

On this mode when you define a processing model, for example:

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
      "id": 1,
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

You define the topic `kafkainput` but really you read from Kafka topic `${APP_ID}_kafkainput` and when you produce to the `output` topic, you really send data to the topic `${APP_ID}_kafkaoutput`. On this mode we define the `APP_ID == TENANT_ID`. To enable this mode you can configure the property `multi.id` to `true`.