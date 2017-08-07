package io.wizzie.ks.cep.builder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;


public class Kafka2Siddhi implements Runnable {

    private final KafkaConsumer kafkaConsumer;

        public Kafka2Siddhi(){

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {

                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
            } finally {
                kafkaConsumer.close();
            }
        }

        public void subscribe(List<String> topics){
            kafkaConsumer.subscribe(topics);
        }

        public void shutdown() {
            kafkaConsumer.wakeup();
        }

}
