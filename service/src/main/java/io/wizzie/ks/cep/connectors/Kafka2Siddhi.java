package io.wizzie.ks.cep.connectors;

import io.wizzie.ks.cep.parsers.EventsParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.*;


public class Kafka2Siddhi implements Runnable {

    private KafkaConsumer consumer;
    private Map<String, String> topics2Siddhi = new HashMap<>();
    Map<String, InputHandler> inputHandlers = new HashMap<>();
    EventsParser eventsParser;

    public Kafka2Siddhi() {
        System.out.println("at constructor");
    }

    private void init() {
        eventsParser = EventsParser.getInstance();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "cep");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        System.out.println("at init");

    }

    @Override
    public void run() {

        init();

        try {
            while (true) {
                ConsumerRecords<String, String> records = null;
                try {
                    records = consumer.poll(100);
                } catch (IllegalStateException e) {
                    //ignore if consumer not subscribed
                }
                if (records != null) {
                    for (ConsumerRecord<String, String> record : records) {
                        for (Map.Entry<String, String> topics2SiddhiEntry : topics2Siddhi.entrySet()) {
                            if (topics2SiddhiEntry.getValue().equals(record.topic()))
                                inputHandlers.get(topics2SiddhiEntry.getValue()).send(eventsParser.parseToObjectArray(topics2SiddhiEntry.getValue(), record.value()));
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

    public void subscribe(Map<String, String> kafka2Siddhi, Map<String, InputHandler> inputHandlers) {
        this.topics2Siddhi.clear();
        this.topics2Siddhi.putAll(kafka2Siddhi);
        this.inputHandlers = inputHandlers;
        consumer.subscribe(Arrays.asList(kafka2Siddhi.keySet().toArray(new String[kafka2Siddhi.keySet().size()])));
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
