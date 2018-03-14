package io.wizzie.ks.cep.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.ks.cep.builder.config.Config;
import io.wizzie.ks.cep.exceptions.SourceNotFoundException;
import io.wizzie.ks.cep.metrics.MetricsManager;
import io.wizzie.ks.cep.model.InOutStreamModel;
import io.wizzie.ks.cep.model.ProcessingModel;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Builder {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);

    Config config;
    KafkaStreams streams;
    MetricsManager metricsManager;

    enum Topics {
        CEP_STREAM_BOOTSTRAPPER_TOPIC("__cep_stream_bootstrapper"),
        CEP_RULES_BOOTSTRAPPER_TOPIC("__cep_rules_bootstrapper");

        private final String topic;

        Topics(final String topic) {
            this.topic = topic;
        }

        @Override
        public String toString() { return topic; }

        public String value() { return topic; }
    }

    public Builder(Config config) throws Exception {
        this.config = config;
        metricsManager = new MetricsManager(config.clone());
        metricsManager.start();
    }

    public void update(String source, String bootstrapConfig) {

        if (streams != null) {
            metricsManager.clean();
            streams.close();
            log.info("Clean CEP process");
        }

        ObjectMapper objectMapper = new ObjectMapper();

        switch (Topics.valueOf(source)) {
            case CEP_RULES_BOOTSTRAPPER_TOPIC:
                try {
                    ProcessingModel processingModel = objectMapper.readValue(bootstrapConfig, ProcessingModel.class);
                    log.info("Processing plan: {}", processingModel);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case CEP_STREAM_BOOTSTRAPPER_TOPIC:
                try {
                    InOutStreamModel inOutStreamModel = objectMapper.readValue(bootstrapConfig, InOutStreamModel.class);
                    log.info("Streams definitions: {}", inOutStreamModel);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            default:
                throw new SourceNotFoundException(String.format("Source \"{}\" not found!", source));
        }

        log.info("Started CEP with conf {}", config.getProperties());
    }

    public void close() {
        metricsManager.interrupt();
        if (streams != null) streams.close();
    }

}
