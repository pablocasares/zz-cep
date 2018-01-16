package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SinkModel extends StreamKafkaModel {
    public SinkModel(@JsonProperty("streamName") String streamName,
                     @JsonProperty("kafkaTopic") String kafkaTopic) {
        super(streamName, kafkaTopic);
    }
}
