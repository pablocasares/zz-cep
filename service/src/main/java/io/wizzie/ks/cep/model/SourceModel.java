package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceModel extends StreamKafkaModel {
    public SourceModel(@JsonProperty("streamName") String streamName,
                       @JsonProperty("kafkaTopic") String kafkaTopic) {
        super(streamName, kafkaTopic);
    }
}
