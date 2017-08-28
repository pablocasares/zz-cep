package io.wizzie.ks.cep.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.ks.cep.controllers.SiddhiController;
import io.wizzie.ks.cep.metrics.MetricsManager;
import io.wizzie.ks.cep.model.ProcessingModel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static io.wizzie.ks.cep.builder.config.ConfigProperties.BOOTSTRAPER_CLASSNAME;
import static io.wizzie.ks.cep.builder.config.ConfigProperties.KAFKA_CLUSTER;


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
        consumerProperties.put("bootstrap.servers", config.get(KAFKA_CLUSTER));
        consumerProperties.put("group.id", "cep");
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", config.get(KAFKA_CLUSTER));
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
