---
layout: page
title: "Stream Configuration"
category: conf
date: 2017-02-14 12:25:04
order: 2
---

The stream configuration is the execution plan of the cep. Cep uses the stream configuration to build the Siddhi processing model using Kafka. You can configure how the cep gets the stream conf to do it you need to use the [Bootstrappers](http://www.wizzie.io/zz-cep/bootstrapper/definition-boostrapper.html).

Stream configuration has two main sections:

### Streams

This section is used to define the streams attributes and types.

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
    ]
}
```

This example defines a stream called `streaminput` with a one field called `timestamp` of type `long`.
The supported attributes types are:
* boolean
* long
* string
* float
* double
* object

You can add the number of streams that you want. You must define an stream if you will use it on the next section.


### Rules

The `rules` section is made by processing rules:

```json
{
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
            "executionPlan": "from streaminput select * insert into streamoutput",
            "options": {"filterOutputNullDimension": true}
        },
        {
            "id": 2,
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

The rules objects are made by: `id`, `version`, `streams` and `executionPlan`

* `id`: The name of the rule.
* `version`: an string that identifies the version of the rule.
* `streams`: a map that contains the `in` and `out` relations between Siddhi and Kafka.
* `executionPlan`: the SiddhiQL query that will be used to process this rule.
* `options`: An optional map field to modify the rule behaviour.

The current `options` available value is:

* `"filterOutputNullDimension"`: This value can be set to `true` if the rule should filter out the null values at the output when the event is sended to Kafka.



So, the full stream configuration should be:

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


Important notes: you must define an stream if you use it at one rule. If you want to update and overwrite a rule, you must send the same rule with a different version. If you send
a rule with the same version, the existing rule will not be overwritten.
The execution plans you can use are defined at [SiddhiQL Documentation](https://wso2.github.io/siddhi/documentation/siddhi-4.0/#query)
