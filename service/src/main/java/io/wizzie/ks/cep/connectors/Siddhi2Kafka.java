package io.wizzie.ks.cep.connectors;

import io.wizzie.ks.cep.controllers.SiddhiController;
import io.wizzie.ks.cep.model.RuleModel;
import io.wizzie.ks.cep.model.SinkModel;
import io.wizzie.ks.cep.parsers.EventsParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;

import java.util.*;

public class Siddhi2Kafka {

    Producer<String, String> producer;
    List<SinkModel> sinks = new LinkedList<>();
    EventsParser eventsParser;
    Map<String, RuleModel> rules = new HashMap<>();
    private static final Logger log = LoggerFactory.getLogger(Siddhi2Kafka.class);


    public Siddhi2Kafka(String kafkaCluster) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaCluster);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        eventsParser = EventsParser.getInstance();
    }


    public void addRules(List<RuleModel> rulesModels) {
        rules.clear();
        log.debug("Adding rules" + rulesModels);
        for (RuleModel rule : rulesModels) {
            rules.put(rule.getId(), rule);
        }
    }

    public void send(String rule, Event event) {
        //iterate over all rules
        log.debug("Current rules: " + rules);
        log.debug("Event rule: " + rule);

        //Get sinks for this rule
        RuleModel ruleModel = rules.get(rule);
        //Send event to all rule sinks
        for (SinkModel sinkModel : ruleModel.getStreams().getSinkModel()){
            producer.send(new ProducerRecord<>(sinkModel.getKafkaTopic(), null, eventsParser.parseToString(sinkModel.getStreamName(), event)));
        }
    }
}