package io.wizzie.cep.connectors.kafka;

import io.wizzie.cep.parsers.EventsParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.wizzie.cep.builder.config.ConfigProperties.APPLICATION_ID;
import static io.wizzie.cep.builder.config.ConfigProperties.MULTI_ID;

public class Siddhi2Kafka {

    Producer<String, Map<String, Object>> producer;
    EventsParser eventsParser;
    private static final Logger log = LoggerFactory.getLogger(Siddhi2Kafka.class);
    private boolean multiId = false;
    private String applicationId;

    public Siddhi2Kafka(Properties producerProperties) {
        producer = new KafkaProducer<>(producerProperties);
        eventsParser = EventsParser.getInstance();
        if(producerProperties.get(MULTI_ID) != null && (Boolean)producerProperties.get(MULTI_ID)){
            this.multiId = (boolean)producerProperties.get(MULTI_ID);
            this.applicationId = (String)producerProperties.get(APPLICATION_ID);
        }
    }


    public void send(String kafkaTopic, String streamName, Event event, Map<String, StreamDefinition> streamDefinitionMap,
                     Map<String, Object> options, Map<String, String> sinkMapper) {

        if(multiId){
            kafkaTopic = String.format("%s_%s",applicationId, kafkaTopic);
        }

        //iterate over all rules
        log.trace("Sending event: " + event);
        log.trace("Event streamName: " + streamName);

        //Send event to kafkaTopic parsing it with the sink stream format
        log.trace("Sending event to topic: " + kafkaTopic);
        List<Attribute> attributeList = streamDefinitionMap.get(streamName).getAttributeList();
        log.trace("Parsed event: " + eventsParser.parseToMap(attributeList, event, options, sinkMapper));

        Map<String, Object> parsedEvent = eventsParser.parseToMap(attributeList, event, options, sinkMapper);
        if (parsedEvent.isEmpty()) {
            log.warn("The parsed event is empty. Not sending it.");
        } else {
            String kafkaKey = (String)parsedEvent.remove("KAFKA_KEY");
            producer.send(new ProducerRecord<>(kafkaTopic, kafkaKey, parsedEvent));
        }
    }


    public void close() {
        producer.close();
    }
}