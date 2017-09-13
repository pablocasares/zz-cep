package io.wizzie.ks.cep.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.ks.cep.controllers.SiddhiController;
import io.wizzie.ks.cep.metrics.MetricsManager;
import io.wizzie.ks.cep.model.ProcessingModel;
import io.wizzie.ks.cep.serializers.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static io.wizzie.ks.cep.builder.config.ConfigProperties.*;


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
        consumerProperties.put("bootstrap.servers", config.get(KAFKA_CLUSTER));
        consumerProperties.put("group.id", config.get(APPLICATION_ID));
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", config.getOrDefault(VALUE_DESERIALIZER, "io.wizzie.ks.cep.serializers.JsonDeserializer"));
        consumerProperties.put(MULTI_ID, config.getOrDefault(MULTI_ID, false));

        Properties producerProperties = new Properties();
        producerProperties.putAll(config.getMapConf());
        producerProperties.put("bootstrap.servers", config.get(KAFKA_CLUSTER));
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", config.getOrDefault(VALUE_SERIALIZER, "io.wizzie.ks.cep.serializers.JsonSerializer"));
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
