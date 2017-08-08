package io.wizzie.ks.cep.connectors;

import io.wizzie.ks.cep.model.RuleModel;
import io.wizzie.ks.cep.model.SinkModel;
import io.wizzie.ks.cep.parsers.EventsParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.wso2.siddhi.core.event.Event;

import java.util.*;

public class Siddhi2Kafka {

    Producer<String, String> producer;
    List<SinkModel> sinks = new LinkedList<>();
    EventsParser eventsParser;
    Map<String, RuleModel> rules = new HashMap<>();

    public Siddhi2Kafka(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
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


    public void addSinks(List<SinkModel> sinks){
        //clear current sinks
        sinks.clear();
        sinks.addAll(sinks);
    }

    public void addRules(Map<String, RuleModel> rules){
        rules.clear();
        rules.putAll(rules);
    }

    public void send(String rule, Event event){

        for(Map.Entry<String, RuleModel> ruleModelEntry: rules.entrySet()){
            if(ruleModelEntry.getKey().equals(rule)){
                for(String stream : ruleModelEntry.getValue().getStreams()){
                    for(SinkModel sinkModel : sinks){
                        if(sinkModel.getStreamName().equals(stream)){
                            producer.send(new ProducerRecord<>(sinkModel.getKafkaTopic(), null, eventsParser.parseToString(sinkModel.getStreamName(), event)));
                        }
                    }
                }
            }
        }
    }
}