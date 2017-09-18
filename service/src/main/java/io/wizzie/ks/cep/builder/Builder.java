package io.wizzie.ks.cep.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.ks.cep.controllers.SiddhiController;
import io.wizzie.ks.cep.metrics.MetricsManager;
import io.wizzie.ks.cep.model.ProcessingModel;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static io.wizzie.ks.cep.builder.config.ConfigProperties.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;


public class Builder implements Listener {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);

    Config config;
    KafkaStreams streams;
    MetricsManager metricsManager;
    Bootstrapper bootstrapper;
    SiddhiController siddhiController;

    public Builder(Config config) throws Exception {
        this.config = config;
        metricsManager = new MetricsManager(config.clone());
        metricsManager.start();


        siddhiController = SiddhiController.getInstance();

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(config.getMapConf());
        consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, config.get(KAFKA_CLUSTER));
        consumerProperties.put(GROUP_ID_CONFIG, config.get(APPLICATION_ID));
        consumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProperties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, config.getOrDefault(VALUE_DESERIALIZER, "io.wizzie.ks.cep.serializers.JsonDeserializer"));
        consumerProperties.put(MULTI_ID, config.getOrDefault(MULTI_ID, false));

        Properties producerProperties = new Properties();
        producerProperties.putAll(config.getMapConf());
        producerProperties.put(BOOTSTRAP_SERVERS_CONFIG, config.get(KAFKA_CLUSTER));
        producerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(PARTITIONER_CLASS_CONFIG, "io.wizzie.ks.cep.connectors.kafka.KafkaPartitioner");
        producerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, config.getOrDefault(VALUE_SERIALIZER, "io.wizzie.ks.cep.serializers.JsonSerializer"));
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
            siddhiController.addProcessingDefinition(processingModel);
            siddhiController.generateExecutionPlans();
            siddhiController.addProcessingModel2KafkaController();
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.info("Started CEP with conf {}", config.getProperties());
    }

    public void close() {
        metricsManager.interrupt();
        if (streams != null) streams.close();
        siddhiController.shutdown();
    }

}
