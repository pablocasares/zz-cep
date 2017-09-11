package io.wizzie.ks.cep.connectors;

import io.wizzie.ks.cep.controllers.SiddhiController;
import io.wizzie.ks.cep.model.RuleModel;
import io.wizzie.ks.cep.model.SinkModel;
import io.wizzie.ks.cep.model.SourceModel;
import io.wizzie.ks.cep.parsers.EventsParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.*;

public class Siddhi2Kafka {

    Producer<String, String> producer;
    EventsParser eventsParser;
    private static final Logger log = LoggerFactory.getLogger(Siddhi2Kafka.class);


    public Siddhi2Kafka(Properties producerProperties) {
        producer = new KafkaProducer<>(producerProperties);
        eventsParser = EventsParser.getInstance();
    }


    public void send(String kafkaTopic, String streamName, Event event, Map<String, StreamDefinition> streamDefinitionMap, Map<String, Object> options) {
        //iterate over all rules
        log.trace("Sending event: " + event);
        log.trace("Event streamName: " + streamName);

        //Send event to kafkaTopic parsing it with the sink stream format
        log.trace("Sending event to topic: " + kafkaTopic);
        List<Attribute> attributeList = streamDefinitionMap.get(streamName).getAttributeList();
        log.trace("Parsed event: " + eventsParser.parseToString(attributeList, event, options));

        String parsedEvent = eventsParser.parseToString(attributeList, event, options);
        if (parsedEvent == null) {
            log.warn("The parsed event is empty. Not sending it.");
        } else {
            producer.send(new ProducerRecord<>(kafkaTopic, null, parsedEvent));
        }
    }


    public void close() {
        producer.close();
    }
}