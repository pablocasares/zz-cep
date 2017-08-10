package io.wizzie.ks.cep.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.wizzie.ks.cep.model.*;
import io.wizzie.ks.cep.serializers.JsonDeserializer;
import io.wizzie.ks.cep.serializers.JsonSerde;
import io.wizzie.ks.cep.serializers.JsonSerializer;
import kafka.utils.MockTime;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class SiddhiControllerIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic("input1", 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic("output1", 4, REPLICATION_FACTOR);
    }

    @Test
    public void SiddhiControllerAddStreamsTest() throws InterruptedException {

        String jsonData = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>("KEY_A", objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.getInstance();
        siddhiController.initKafkaController(CLUSTER.bootstrapServers());

        //Add Stream Definition

        SourceModel sourceModel = new SourceModel("stream","input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput","input1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        AttributeModel attributeModel = new AttributeModel("attributeName", "string");
        List<AttributeModel> attributeModelList = new LinkedList<>();
        attributeModelList.add(attributeModel);
        StreamModel streamModel = new StreamModel("stream",attributeModelList);
        List<StreamModel> streamModelList = new LinkedList<>();
        streamModelList.add(streamModel);



        InOutStreamModel inOutStreamModel = new InOutStreamModel(sourceModelList, sinkModelList, streamModelList);
        siddhiController.addStreamDefinition(inOutStreamModel);
        siddhiController.generateExecutionPlans();
        //////////////////////////////////

        //Add Rule Definition

        String id = "rule1";
        String version = "v1";
        List<String> streams = Collections.singletonList("stream");
        String executionPlan = "from stream select * insert into streamoutput";

        RuleModel ruleModelObject = new RuleModel(id, version, streams, executionPlan);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);

        ProcessingModel processingModel = new ProcessingModel(ruleModelList);
        siddhiController.addRulesDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        //////////////////////////////////


        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);


//        Map<String, Object> expectedData = new HashMap<>();
//        expectedData.put("X", "VALUE");
//        expectedData.put("timestamp", 1473316426);
//
//        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>("VALUE", expectedData);

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously("input1", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output1", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        //assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @AfterClass
    public static void stop() {
        CLUSTER.stop();
    }


}
