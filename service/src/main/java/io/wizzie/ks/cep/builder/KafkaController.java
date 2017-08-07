package io.wizzie.ks.cep.builder;

import io.wizzie.ks.cep.model.InOutStreamModel;
import io.wizzie.ks.cep.model.SourceModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaController {


    private KafkaConsumer<String, String> consumer;
    private Map<String, String> kafka2Siddhi = new HashMap<>();
    final List<Kafka2Siddhi> consumers = new ArrayList<>();

    public KafkaController() {

        int numConsumers = 1;
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        for (int i = 0; i < numConsumers; i++) {
            Kafka2Siddhi consumer = new Kafka2Siddhi();
            consumers.add(consumer);
            executor.submit(consumer);
        }
    }


    public void addSourcesStream(InOutStreamModel inOutStreamModel) {

        //clear existing sources
        kafka2Siddhi.clear();

        for (SourceModel sourceModel : inOutStreamModel.getSources()) {
            kafka2Siddhi.put(sourceModel.getKafkaTopic(), sourceModel.getStreamName());
        }

        for (Kafka2Siddhi consumer : consumers) {
            //Subscribe to the topics associated with the streams.
            consumer.subscribe(Arrays.asList(kafka2Siddhi.keySet().toArray(new String[kafka2Siddhi.keySet().size()])), kafka2Siddhi);
        }
    }

}
