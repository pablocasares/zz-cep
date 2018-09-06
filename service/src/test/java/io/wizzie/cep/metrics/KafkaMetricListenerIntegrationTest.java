package io.wizzie.cep.metrics;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.cep.builder.Builder;
import io.wizzie.cep.builder.config.ConfigProperties;
import io.wizzie.cep.serializers.JsonDeserializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertTrue;


public class KafkaMetricListenerIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC = "__metrics";
    private static final String DUMMY_TOPIC = "dummy_topic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic(INPUT_TOPIC, 1, REPLICATION_FACTOR);
        CLUSTER.createTopic(DUMMY_TOPIC, 1, REPLICATION_FACTOR);
    }

    @Test
    public void kafkaMetricListenerShouldWork() throws Exception {

        Map<String, Object> cepConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        cepConfiguration.put(ConfigProperties.APPLICATION_ID, appId);
        cepConfiguration.put(ConfigProperties.KAFKA_CLUSTER, CLUSTER.bootstrapServers());
        cepConfiguration.put(ConfigProperties.VALUE_SERIALIZER, StringSerializer.class);
        cepConfiguration.put(ConfigProperties.VALUE_DESERIALIZER, JsonDeserializer.class);

        Config config = new Config(cepConfiguration);
        config.put("metric.interval", 2000);
        config.put("metric.listeners", Collections.singletonList("io.wizzie.metrics.listeners.KafkaMetricListener"));
        config.put("metric.enable", true);
        config.put("file.bootstraper.path", Thread.currentThread().getContextClassLoader().getResource("sample_config.json").getFile());
        config.put(ConfigProperties.BOOTSTRAPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper");

        Builder builder = new Builder(config);

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<KeyValue<String, Map>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, INPUT_TOPIC, 1);

        assertTrue(!result.isEmpty());

        builder.close();
    }

}
