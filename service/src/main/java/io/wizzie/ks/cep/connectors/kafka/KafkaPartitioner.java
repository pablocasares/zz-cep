package io.wizzie.cep.connectors.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.Random;

public class KafkaPartitioner implements Partitioner {

    Random r;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(keyBytes == null){
            return r.nextInt(cluster.partitionCountForTopic(topic));
        }else {
            return Utils.abs(Utils.murmur2(keyBytes)) % cluster.partitionCountForTopic(topic);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        r = new Random();
    }
}
