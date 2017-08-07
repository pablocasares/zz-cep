package io.wizzie.ks.cep.builder;

import io.wizzie.ks.cep.model.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaController {


    private KafkaConsumer<String, String> consumer;
    private Map<String, String> kafka2Siddhi = new HashMap<>();
    private Map<String, String> siddhi2Kafka = new HashMap<>();
    final List<Kafka2Siddhi> consumers = new ArrayList<>();
    Siddhi2Kafka producer;

    public KafkaController() {

        int numConsumers = 1;
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        for (int i = 0; i < numConsumers; i++) {
            Kafka2Siddhi consumer = new Kafka2Siddhi();
            consumers.add(consumer);
            executor.submit(consumer);
        }

        producer = new Siddhi2Kafka();
    }


    public void addSources2Stream(InOutStreamModel inOutStreamModel, Map<String, InputHandler> inputHandlers) {

        //clear existing sources
        kafka2Siddhi.clear();

        for (SourceModel sourceModel : inOutStreamModel.getSources()) {
            kafka2Siddhi.put(sourceModel.getKafkaTopic(), sourceModel.getStreamName());
        }

        for (Kafka2Siddhi consumer : consumers) {
            //Subscribe to the topics associated with the streams.
            consumer.subscribe(kafka2Siddhi, inputHandlers);
        }
    }


    public void addStream2Sinks(InOutStreamModel inOutStreamModel, Map<String, RuleModel> rules){

        //clear existing sinks
        siddhi2Kafka.clear();

        producer.addSinks(inOutStreamModel.getSinks());
        producer.addRules(rules);

    }

    public void send2Kafka(String rule,Event event){
        producer.send(rule, event);
    }

}
