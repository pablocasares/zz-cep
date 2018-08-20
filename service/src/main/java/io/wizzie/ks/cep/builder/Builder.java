package io.wizzie.cep.builder;

import com.codahale.metrics.JmxAttributeGauge;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.cep.controllers.SiddhiController;
import io.wizzie.cep.model.SiddhiAppBuilder;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.cep.model.ProcessingModel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.Properties;

import static io.wizzie.cep.builder.config.ConfigProperties.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;


public class Builder implements Listener {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);

    Config config;
    KafkaStreams streams;
    MetricsManager metricsManager;
    Bootstrapper bootstrapper;
    SiddhiController siddhiController;
    SiddhiAppBuilder siddhiAppBuilder;

    //Prevent invalid instantiation
    private Builder(){
    }

    //Use this constructor for production code
    public Builder(Config config) throws Exception {
        startBuilder(config);
    }

    //Use this constructor for testing code in order to avoid collisions with SiddhiController static instances
    public Builder(Config config, SiddhiController siddhiController) throws Exception {
        this.siddhiController = siddhiController;
        startBuilder(config);
    }

    private void startBuilder(Config config) throws Exception {
        this.siddhiAppBuilder = new SiddhiAppBuilder();
        this.config = config;
        metricsManager = new MetricsManager(config.getMapConf());
        metricsManager.start();

        if(siddhiController == null) {
            siddhiController = SiddhiController.getInstance();
        }

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(config.getMapConf());
        consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, config.get(KAFKA_CLUSTER));

        consumerProperties.put(GROUP_ID_CONFIG,  String.format("%s_%s", config.get(APPLICATION_ID), "zz-cep"));
        consumerProperties.put(CLIENT_ID_CONFIG, String.format("%s_%s", config.get(APPLICATION_ID), "zz-cep"));
        consumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, config.getOrDefault(VALUE_DESERIALIZER, "io.wizzie.cep.serializers.JsonDeserializer"));
        consumerProperties.put(MULTI_ID, config.getOrDefault(MULTI_ID, false));

        Properties producerProperties = new Properties();
        producerProperties.putAll(config.getMapConf());
        producerProperties.put(BOOTSTRAP_SERVERS_CONFIG, config.get(KAFKA_CLUSTER));
        producerProperties.put(CLIENT_ID_CONFIG, String.format("%s_%s", config.get(APPLICATION_ID), "zz-cep"));
        producerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(PARTITIONER_CLASS_CONFIG, "io.wizzie.cep.connectors.kafka.KafkaPartitioner");
        producerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, config.getOrDefault(VALUE_SERIALIZER, "io.wizzie.cep.serializers.JsonSerializer"));
        producerProperties.put(MULTI_ID, config.getOrDefault(MULTI_ID, false));
        siddhiController.initKafkaController(consumerProperties, producerProperties);

        bootstrapper = BootstrapperBuilder.makeBuilder()
                .boostrapperClass(config.get(BOOTSTRAPER_CLASSNAME))
                .withConfigInstance(config)
                .listener(this)
                .build();
    }

    @Override
    public void updateConfig(SourceSystem sourceSystem, String bootstrapConfig) {
        if (streams != null) {
            metricsManager.clean();
            streams.close();
            log.info("Clean CEP process");
        }

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            ProcessingModel processingModel = objectMapper.readValue(bootstrapConfig, ProcessingModel.class);
            log.info("Processing plan: {}", processingModel);
            if(siddhiAppBuilder.validateSiddhiPlan(processingModel)) {
                siddhiController.addProcessingDefinition(processingModel);
                siddhiController.generateExecutionPlans();
                siddhiController.addProcessingModel2KafkaController();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        registerKafkaMetrics(config, metricsManager);
        log.info("Started CEP with conf {}", config.getProperties());
    }

    private void registerKafkaMetrics(Config config, MetricsManager metricsManager){
        String appId = config.get(APPLICATION_ID_CONFIG);

        log.info("Register kafka jvm metrics: ");

            try {

                // PRODUCER
                log.info(" * {}", "producer.messages_send_per_sec");
                metricsManager.registerMetric("producer.messages_send_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "zz-cep")), "record-send-rate"));

                log.info(" * {}", "producer.output_bytes_per_sec");
                metricsManager.registerMetric("producer.output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "zz-cep")), "outgoing-byte-rate"));

                log.info(" * {}", "producer.incoming_bytes_per_sec");
                metricsManager.registerMetric("producer.incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "zz-cep")), "incoming-byte-rate"));

                // CONSUMER
                log.info(" * {}", "consumer.max_lag");
                metricsManager.registerMetric("consumer.max_lag",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + String.format("%s_%s", appId, "zz-cep")), "records-lag-max"));

                log.info(" * {}", "consumer.output_bytes_per_sec");
                metricsManager.registerMetric("consumer.output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + String.format("%s_%s", appId, "zz-cep")), "outgoing-byte-rate"));

                log.info(" * {}", "consumer.incoming_bytes_per_sec");
                metricsManager.registerMetric("consumer.incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + String.format("%s_%s", appId, "zz-cep")), "incoming-byte-rate"));

                log.info(" * {}", "consumer.records_per_sec");
                metricsManager.registerMetric("consumer.records_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + String.format("%s_%s", appId, "zz-cep")), "records-consumed-rate"));

            } catch (MalformedObjectNameException e) {
                log.error("kafka jvm metrics not found", e);
            }
    }

    public void close() throws Exception {
        metricsManager.interrupt();
        bootstrapper.close();
        if (streams != null) streams.close();
        siddhiController.shutdown();
    }

}
