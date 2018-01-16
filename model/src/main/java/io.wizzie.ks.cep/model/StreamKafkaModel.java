package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamKafkaModel {
    String streamName;
    String kafkaTopic;

    @JsonCreator
    public StreamKafkaModel(@JsonProperty("streamName") String streamName,
                            @JsonProperty("kafkaTopic") String kafkaTopic) {
        this.streamName = streamName;
        this.kafkaTopic = kafkaTopic;
    }

    @JsonProperty
    public String getStreamName() {
        return streamName;
    }

    @JsonProperty
    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @JsonProperty
    public String getKafkaTopic() {
        return kafkaTopic;
    }

    @JsonProperty
    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("streamName: ").append(streamName).append(", ")
                .append("kafkaTopic: ").append(kafkaTopic)
                .append("}");

        return sb.toString();
    }
}
