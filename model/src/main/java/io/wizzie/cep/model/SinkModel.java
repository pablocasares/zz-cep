package io.wizzie.cep.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class SinkModel extends StreamKafkaModel {
    public SinkModel(@JsonProperty("streamName") String streamName,
                     @JsonProperty("kafkaTopic") String kafkaTopic,
                     @JsonProperty("dimMapper") Map<String, String> dimMapper) {
        super(streamName, kafkaTopic, dimMapper);
    }
}
