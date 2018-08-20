package io.wizzie.cep.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.cep.model.*;
import io.wizzie.cep.serializers.JsonDeserializer;
import io.wizzie.cep.serializers.JsonSerializer;
import kafka.utils.MockTime;
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

import static io.wizzie.cep.builder.config.ConfigProperties.APPLICATION_ID;
import static io.wizzie.cep.builder.config.ConfigProperties.MULTI_ID;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

public class SiddhiControllerIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final Properties consumerNoMultiIdProperties = new Properties();
    private static final Properties producerNoMultiIdProperties = new Properties();


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic("input1", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input2", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input3", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input4", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input5", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input6", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input7", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input8", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input9", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("aabb_input10", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input11", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input12", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input13", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("input14", 1, REPLICATION_FACTOR);



        // sinks
        CLUSTER.createTopic("output1", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output3", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output4", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output5", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output6", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output66", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output77", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output8", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output9", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("aabb_output10", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output11", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output12", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output13", 1, REPLICATION_FACTOR);
        CLUSTER.createTopic("output14", 1, REPLICATION_FACTOR);


        consumerNoMultiIdProperties.put("bootstrap.servers", CLUSTER.bootstrapServers());
        consumerNoMultiIdProperties.put("group.id", "zz-cep-test");
        consumerNoMultiIdProperties.put("enable.auto.commit", "true");
        consumerNoMultiIdProperties.put("auto.commit.interval.ms", "1000");
        consumerNoMultiIdProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerNoMultiIdProperties.put("value.deserializer", "io.wizzie.cep.serializers.JsonDeserializer");
        //Property just needed for testing.
        consumerNoMultiIdProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        producerNoMultiIdProperties.put("bootstrap.servers", CLUSTER.bootstrapServers());
        producerNoMultiIdProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerNoMultiIdProperties.put("value.serializer", "io.wizzie.cep.serializers.JsonSerializer");
        producerNoMultiIdProperties.put(PARTITIONER_CLASS_CONFIG, "io.wizzie.cep.connectors.kafka.KafkaPartitioner");


    }

    @Test
    public void SiddhiControllerAddTwoStreamTest() throws InterruptedException {

        String jsonData = "{\"attributeName\":\"VALUE\"}";
        String jsonData2 = "{\"attributeName\":\"VALUE2\"}";


        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        KeyValue<String, Map<String, Object>> kvStream2 = null;

        try {
            kvStream2 = new KeyValue<>(null, objectMapper.readValue(jsonData2, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        SourceModel sourceModel = new SourceModel("stream1", "input1", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput1", "output1", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule1";
        String version = "v1";
        String executionPlan = "from stream1 select * insert into streamoutput1";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream1", Arrays.asList(
                        new AttributeModel("attributeName", "string")
                )), new StreamModel("stream11", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("attributeName", "VALUE2");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);
        KeyValue<String, Map<String, Object>> expectedDataKv2 = new KeyValue<>(null, expectedData2);

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        try {
            System.out.println("Producing KVs: " + kvStream1 + kvStream2);
            IntegrationTestUtils.produceKeyValuesSynchronously("input1", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously("input1", Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);

        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output1", 2);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Arrays.asList(expectedDataKv, expectedDataKv2), receivedMessagesFromOutput1);

    }


    @Test
    public void SiddhiControllerAddOneStreamTest() throws InterruptedException {


        String jsonData = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream2", "input2", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput2", "output2", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule2";
        String version = "v1";
        String executionPlan = "from stream2 select * insert into streamoutput2";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream2", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input2", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output2", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }


    @Test
    public void SiddhiControllerAddTwoRulesDifferentStreamsTest() throws InterruptedException {

        String jsonData = "{\"attributeName\":\"VALUE\"}";
        String jsonData2 = "{\"attributeName\":\"VALUE2\"}";


        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        KeyValue<String, Map<String, Object>> kvStream2 = null;

        try {
            kvStream2 = new KeyValue<>(null, objectMapper.readValue(jsonData2, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream3", "input3", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        SourceModel sourceModel2 = new SourceModel("stream33", "input3", null);
        sourceModelList.add(sourceModel);
        sourceModelList.add(sourceModel2);

        SinkModel sinkModel = new SinkModel("streamoutput3", "output3", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule3";
        String version = "v1";
        String executionPlan = "from stream3 select * insert into streamoutput3";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        String id2 = "rule33";
        String version2 = "v1";
        String executionPlan2 = "from stream33 select * insert into streamoutput3";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream3", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )), new StreamModel("stream33", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("attributeName", "VALUE2");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);
        KeyValue<String, Map<String, Object>> expectedDataKv2 = new KeyValue<>(null, expectedData2);


        try {
            System.out.println("Producing KVs: " + kvStream1 + kvStream2);
            IntegrationTestUtils.produceKeyValuesSynchronously("input3", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously("input3", Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);

        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output3", 2);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Arrays.asList(expectedDataKv, expectedDataKv2), receivedMessagesFromOutput1);

    }


    @Test
    public void SiddhiControllerAddAvgRuleStreamTest() throws InterruptedException {

        String jsonData = "{\"attributeName\":2}";


        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream4", "input4", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput4", "output4", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule4";
        String version = "v1";
        String executionPlan = "from stream4 select avg(attributeName) as avg insert into streamoutput4";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream4", Arrays.asList(
                        new AttributeModel("attributeName","long")
                )), new StreamModel("stream44", Arrays.asList(
                        new AttributeModel("attributeName","long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("avg", 2.0);


        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);


        try {
            System.out.println("Producing KVs: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input4", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output4", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Arrays.asList(expectedDataKv), receivedMessagesFromOutput1);

    }


    @Test
    public void SiddhiControllerAddPartitionRuleStreamTest() throws InterruptedException {

        String jsonData = "{\"fieldA\":3, \"fieldB\":2}";


        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("streaminput5", "input5", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput5", "output5", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule5";
        String version = "v1";
        String executionPlan = "partition with (fieldA of streaminput5) begin from streaminput5#window.timeBatch(5 sec) select avg(fieldB) as avg insert into streamoutput5 end";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streaminput5", Arrays.asList(
                        new AttributeModel("fieldA","long"),
                        new AttributeModel("fieldB","long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("avg", 2.0);


        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);


        try {
            System.out.println("Producing KVs: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input5", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output5", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Arrays.asList(expectedDataKv), receivedMessagesFromOutput1);

    }


    @Test
    public void SiddhiControllerAddOutputNotUsedRuleStreamTest() throws InterruptedException {

        String jsonData = "{\"fieldA\":\"yes\", \"fieldB\": 0}";
        String jsonData2 = "{\"fieldA\":\"no\", \"fieldB\":2}";


        KeyValue<String, Map<String, Object>> kvStream1 = null;
        KeyValue<String, Map<String, Object>> kvStream2 = null;


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
            kvStream2 = new KeyValue<>(null, objectMapper.readValue(jsonData2, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("streaminput6", "input6", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput6", "output6", null);
        SinkModel sinkModel2 = new SinkModel("streamoutput66", "output66", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);
        sinkModelList.add(sinkModel2);

        String id = "rule6";
        String version = "v1";
        String executionPlan = "from every e1=streaminput6[fieldA == 'yes'] -> e2=streaminput6[fieldB > 0] select 'Correct' as description insert into streamoutput6";
        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streaminput6", Arrays.asList(
                        new AttributeModel("fieldA","string"),
                        new AttributeModel("fieldB","long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("description", "Correct");


        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);


        try {
            System.out.println("Producing KVs: " + kvStream1 + kvStream2);
            IntegrationTestUtils.produceKeyValuesSynchronously("input6", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously("input6", Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output6", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Arrays.asList(expectedDataKv), receivedMessagesFromOutput1);

    }


    @Test
    public void SiddhiControllerSend2TwoOutputsStreamTest() throws InterruptedException {

        String jsonData = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("streaminput7", "input7", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput7", "output7", null);
        SinkModel sinkModel2 = new SinkModel("streamoutput7", "output77", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);
        sinkModelList.add(sinkModel2);

        String id = "rule6";
        String version = "v1";
        String executionPlan = "from streaminput7 select * insert into streamoutput7";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);
        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streaminput7", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input7", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output7", 1);
        List<KeyValue<String, Map>> receivedMessagesFromOutput2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output77", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput2);

        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput2);

    }

    @Test
    public void SiddhiControllerShouldFilterNullsStreamTest() throws InterruptedException {

        String jsonData = "{\"attributeName\":2}";


        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream8", "input8", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput8", "output8", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule8";
        String version = "v1";
        String executionPlan = "from stream8 select * insert into streamoutput8";
        Map<String, Object> options = new HashMap<>();
        options.put("filterOutputNullDimension", true);

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, options);


        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream8", Arrays.asList(
                        new AttributeModel("attributeName","long"),
                        new AttributeModel("attributeName2","long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", 2L);


        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);


        try {
            System.out.println("Producing KVs: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input8", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output8", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Arrays.asList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerShouldNotSendAnEmptyMessageStreamTest() throws InterruptedException {

        String jsonData = "{\"attributeNotUsed\":2}";
        String jsonData2 = "{\"attributeName\":2}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;
        KeyValue<String, Map<String, Object>> kvStream2 = null;


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
            kvStream2 = new KeyValue<>(null, objectMapper.readValue(jsonData2, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream9", "input9", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput9", "output9", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule9";
        String version = "v1";
        String executionPlan = "from stream9 select * insert into streamoutput9";
        Map<String, Object> options = new HashMap<>();
        options.put("filterOutputNullDimension", true);

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, options);


        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream9", Arrays.asList(
                        new AttributeModel("attributeName","long"),
                        new AttributeModel("attributeName2","long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", 2L);


        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);


        try {
            System.out.println("Producing KVs: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input9", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously("input9", Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output9", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Arrays.asList(expectedDataKv), receivedMessagesFromOutput1);

    }


    @Test
    public void SiddhiControllerAddOneStreamWithMultiIdTest() throws InterruptedException {

        String jsonData = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        Properties internalConsumerProperties = new Properties();
        internalConsumerProperties.put("bootstrap.servers", CLUSTER.bootstrapServers());
        internalConsumerProperties.put("group.id", "cep");
        internalConsumerProperties.put("enable.auto.commit", "true");
        internalConsumerProperties.put("auto.commit.interval.ms", "1000");
        internalConsumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        internalConsumerProperties.put("value.deserializer", "io.wizzie.cep.serializers.JsonDeserializer");
        internalConsumerProperties.put(APPLICATION_ID, "aabb");
        internalConsumerProperties.put(MULTI_ID, true);
        //Property just needed for testing.
        internalConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Properties internalProducerProperties = new Properties();
        internalProducerProperties.put("bootstrap.servers", CLUSTER.bootstrapServers());
        internalProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        internalProducerProperties.put("value.serializer", "io.wizzie.cep.serializers.JsonSerializer");
        internalProducerProperties.put(PARTITIONER_CLASS_CONFIG, "io.wizzie.cep.connectors.kafka.KafkaPartitioner");
        internalProducerProperties.put(APPLICATION_ID, "aabb");
        internalProducerProperties.put(MULTI_ID, true);


        siddhiController.initKafkaController(internalConsumerProperties, internalProducerProperties);

        SourceModel sourceModel = new SourceModel("stream10", "input10", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput10", "output10", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule10";
        String version = "v1";
        String executionPlan = "from stream10 select * insert into streamoutput10";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream10", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("aabb_input10", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "aabb_output10", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerSendMalformedMessageStreamTest() throws InterruptedException {


        String jsonDataMalformed = "{\"attributeName\":\"VALUE\"";
        String jsonDataWellformed = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, String> kvStream1;
        KeyValue<String, String> kvStream2;
        kvStream1 = new KeyValue<>(null, jsonDataMalformed);
        kvStream2 = new KeyValue<>(null, jsonDataWellformed);


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream11", "input11", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput11", "output11", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule11";
        String version = "v1";
        String executionPlan = "from stream11 select * insert into streamoutput11";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream11", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input11", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
            IntegrationTestUtils.produceKeyValuesSynchronously("input11", Collections.singletonList(kvStream2), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output11", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerBypassKafkaStreamTest() throws InterruptedException {


        String jsonData = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, String> kvStream1;
        kvStream1 = new KeyValue<>(null, jsonData);


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream12", "input12", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput12", "output12", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule12";
        String version = "v1";
        String executionPlan = "from stream12 select * insert into streamoutput12";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream12", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input12", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output12", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerBypassAndOverwriteKafkaStreamTest() throws InterruptedException {


        String jsonData = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, String> kvStream1;
        kvStream1 = new KeyValue<>(null, jsonData);


        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream13", "input13", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput13", "output13", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule13";
        String version = "v1";
        String executionPlan = "from stream13 select attributeName, attributeName as KAFKA_KEY insert into streamoutput13";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream13", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>("VALUE", expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input13", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output13", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerCorrectDeserializationTest() throws InterruptedException {


        String jsonData = "{\"timestamp\":1,\"bytes\":30}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream14", "input14", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput14", "output14", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule14";
        String version = "v1";
        String executionPlan = "from stream14#window.timeBatch(1 sec) select timestamp, sum(bytes) as sumbytes insert into streamoutput14";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream14", Arrays.asList(
                        new AttributeModel("timestamp","long"),
                        new AttributeModel("bytes","long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("sumbytes", 30L);
        expectedData.put("timestamp", 1L);

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input14", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output14", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerAttributeWithDashAtInput() throws InterruptedException {
  
        String jsonData = "{\"attribute-Name\":\"VALUE\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        Map<String, String> sourceDimMapper = new HashMap<>();
        sourceDimMapper.put("attributeName", "attribute-Name");
        SourceModel sourceModel = new SourceModel("stream15", "input15", sourceDimMapper);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput15", "output15", null);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule15";
        String version = "v1";
        String executionPlan = "from stream15 select * insert into streamoutput15";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream15", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attributeName", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input15", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output15", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerAttributeWithDashAtInputAndOutput() throws InterruptedException {


        String jsonData = "{\"attribute-Name\":\"VALUE\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        Map<String, String> sourceDimMapper = new HashMap<>();
        sourceDimMapper.put("attributeName", "attribute-Name");
        SourceModel sourceModel = new SourceModel("stream16", "input16", sourceDimMapper);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        Map<String, String> sinkDimMapper = new HashMap<>();
        sinkDimMapper.put("attributeName", "attribute-Name");
        SinkModel sinkModel = new SinkModel("streamoutput16", "output16", sinkDimMapper);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule16";
        String version = "v1";
        String executionPlan = "from stream16 select * insert into streamoutput16";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream16", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("attribute-Name", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input16", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output16", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }

    @Test
    public void SiddhiControllerAttributeWithDashAtOutput() throws InterruptedException {


        String jsonData = "{\"attributeName\":\"VALUE\"}";

        KeyValue<String, Map<String, Object>> kvStream1 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            kvStream1 = new KeyValue<>(null, objectMapper.readValue(jsonData, Map.class));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();
        siddhiController.initKafkaController(consumerNoMultiIdProperties, producerNoMultiIdProperties);

        SourceModel sourceModel = new SourceModel("stream17", "input17", null);
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        Map<String, String> sinkDimMapper = new HashMap<>();
        sinkDimMapper.put("attributeName", "renamedField");
        SinkModel sinkModel = new SinkModel("streamoutput17", "output17", sinkDimMapper);
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule17";
        String version = "v1";
        String executionPlan = "from stream17 select * insert into streamoutput17";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream17", Arrays.asList(
                        new AttributeModel("attributeName","string")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();
        siddhiController.addProcessingModel2KafkaController();

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-A");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfigA.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("renamedField", "VALUE");

        KeyValue<String, Map<String, Object>> expectedDataKv = new KeyValue<>(null, expectedData);

        try {
            System.out.println("Producing KV: " + kvStream1);
            IntegrationTestUtils.produceKeyValuesSynchronously("input17", Collections.singletonList(kvStream1), producerConfig, MOCK_TIME);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, "output17", 1);
        System.out.println("Received after Siddhi: " + receivedMessagesFromOutput1);
        assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput1);

    }
  
    @AfterClass
    public static void stop() {
    }
}
