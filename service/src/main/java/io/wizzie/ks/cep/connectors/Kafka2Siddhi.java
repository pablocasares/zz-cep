package io.wizzie.ks.cep.connectors;


import io.wizzie.ks.cep.parsers.EventsParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.stream.input.InputHandler;

import static io.wizzie.ks.cep.builder.config.ConfigProperties.*;

import java.util.*;
import java.util.concurrent.Semaphore;


public class Kafka2Siddhi implements Runnable {

    private KafkaConsumer consumer;
    private Map<String, String> topics2Siddhi = new HashMap<>();
    Map<String, Map<String, InputHandler>> inputHandlers = new HashMap<>();
    EventsParser eventsParser;
    Semaphore mutex;
    private static final Logger log = LoggerFactory.getLogger(Kafka2Siddhi.class);
    private boolean multiId = false;
    private String applicationId;


    public Kafka2Siddhi(Properties consumerProperties) {
        mutex = new Semaphore(1);
        eventsParser = EventsParser.getInstance();
        this.consumer = new KafkaConsumer<>(consumerProperties);
        if (consumerProperties.get(MULTI_ID) != null && (boolean) consumerProperties.get(MULTI_ID)) {
            this.multiId = (Boolean) consumerProperties.get(MULTI_ID);
            this.applicationId = (String) consumerProperties.get(APPLICATION_ID);
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                log.trace("Consumer acquiring mutex");
                mutex.acquire();
                ConsumerRecords<String, Map<String, Object>> records = null;
                try {
                    log.trace("Consumer starts poll. It will stay at this line if the consumer can't connect to Kafka.");
                    records = consumer.poll(100);
                } catch (IllegalStateException e) {
                    //ignore if consumer not subscribed
                } finally {
                }

                if (records != null) {
                    //iterate over received events
                    for (ConsumerRecord<String, Map<String, Object>> record : records) {
                        log.debug("Consumed event: " + record.key() + " --> " + record.value());
                        //iterate over topics-->stream names relations
                        log.debug("Current topics2Siddhi relations: " + topics2Siddhi.toString());
                        for (Map.Entry<String, String> topics2SiddhiEntry : topics2Siddhi.entrySet()) {
                            //if the received event belongs to a concrete topic
                            if (topics2SiddhiEntry.getKey().equals(record.topic())) {
                                log.debug("Received event belogs to stream: " + topics2SiddhiEntry.getValue());
                                //iterate over all rules --> inputhandlers relations
                                for (Map.Entry<String, Map<String, InputHandler>> inputHandlersEntry : inputHandlers.entrySet()) {
                                    //iterate over streams --> inputhandlers relations
                                    for (Map.Entry<String, InputHandler> stream2InputHandler : inputHandlersEntry.getValue().entrySet()) {
                                        //if this topic belongs to this stream2InputHandler send it:
                                        if (stream2InputHandler.getKey().equals(topics2SiddhiEntry.getValue())) {
                                            log.debug("This event from topic: " + record.topic() + " belongs to stream: " + stream2InputHandler.getKey() + ". Sending it to: " + stream2InputHandler.getValue().toString());
                                            if (record.value() != null) {
                                                Object[] data = eventsParser.parseToObjectArray(topics2SiddhiEntry.getValue(), record.value());
                                                stream2InputHandler.getValue().send(data);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                log.trace("Consumer releasing mutex");
                mutex.release();
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

    public void subscribe(Map<String, String> kafka2Siddhi, Map<String, Map<String, InputHandler>> inputHandlers) {
        log.debug("Subscribing");
        try {
            log.debug("Subscriber acquiring mutex");
            mutex.acquire();
            log.debug("Subscriber entered exclusion zone");
            this.topics2Siddhi.clear();
            this.topics2Siddhi.putAll(kafka2Siddhi);
            this.inputHandlers = inputHandlers;
            log.debug("Pausing consumer");
            log.debug("Subscribing to: " + topics2Siddhi.keySet());
            List<String> topics = Arrays.asList(topics2Siddhi.keySet().toArray(new String[topics2Siddhi.keySet().size()]));
            List<String> topicsWithMultiId = new LinkedList<>();


            Map<String, String> newTopics2Siddhi = new HashMap<>();
            if (multiId) {
                topics.forEach(topic -> {
                    topicsWithMultiId.add(String.format("%s_%s", applicationId, topic));
                    newTopics2Siddhi.put(String.format("%s_%s", applicationId, topic), topics2Siddhi.get(topic));
                });
                topics2Siddhi.clear();
                topics2Siddhi.putAll(newTopics2Siddhi);
                consumer.subscribe(topicsWithMultiId);
            } else {
                consumer.subscribe(topics);
            }

            log.debug("Resuming consumer: " + consumer.assignment());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            log.debug("Subscriber releasing mutex");
            mutex.release();
        }
        log.debug("Subscribed");
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
