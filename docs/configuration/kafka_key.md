---
title: Kafka Key
layout: single
toc: true
---


The CEP is able to bypass the incoming Kafka key through Siddhi. This is how it is made:

When the CEP read a Kafka message that is keyed:

```
aabb,{"timestamp":1234, "value":3}
```
It inserts the key ("aabb") as an event field with the key "KAFKA_KEY". The event will be readed by Siddhi as:

```json
{"timestamp":1234, "value":3, "KAFKA_KEY":"aabb"}
```

Also, when you define an stream:

```json
    {
      "streamName": "streaminput",
      "attributes": [
        {
          "name": "timestamp",
          "type": "long"
        },
        {
          "name": "value",
          "type": "integer"
        }
      ]
    }
```

The actual stream that is defined is:

```json
    {
      "streamName": "streaminput",
      "attributes": [
        {
          "name": "timestamp",
          "type": "long"
        },
        {
          "name": "value",
          "type": "integer"
        },
        {
          "name": "KAFKA_KEY",
          "type": "string"
        }
      ]
    }
```

This allows the CEP to pass the key through Siddhi core.


Finally, when Siddhi sends the output to Kafka, the `"KAFKA_KEY"` field will be removed from the event and will be used as the key.

So, if you send the next event with the following processing definition:

```json
{
  "streams": [
    {
      "streamName": "streaminput",
      "attributes": [
        {
          "name": "timestamp",
          "type": "long"
        },
        {
          "name": "value",
          "type": "integer"
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

And you send:

```json
aabb,{"timestamp":1234, "value":3}
```

You will get at the output kafka topic:

```json
aabb,{"timestamp":1234, "value":3}
```

The user has to ensure that the KAFKA_KEY field is selected to be at the Siddhi output. If it isn't selected the KAFKA_KEY will not be used to write the message to Kafka.


## Overwriting the KAFKA_KEY

You can also use other fields of the message as the KAKFA_KEY. If you use a query like this:

```
"from stream select timestamp, value, value as KAFKA_KEY insert into streamoutput"

```

The field "value" will be used as the KAFKA_KEY. So the output will be:

```json
3,{"timestamp":1234, "value":3}
```

