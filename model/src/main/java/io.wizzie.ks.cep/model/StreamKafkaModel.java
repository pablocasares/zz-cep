package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class StreamKafkaModel {
    String streamName;
    String kafkaTopic;
    Map<String, String> dimMapper;

    @JsonCreator
    public StreamKafkaModel(@JsonProperty("streamName") String streamName,
                            @JsonProperty("kafkaTopic") String kafkaTopic,
                            @JsonProperty("dimMapper") Map<String, String> dimMapper) {
        this.streamName = streamName;
        this.kafkaTopic = kafkaTopic;
        this.dimMapper = dimMapper;

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
    public Map<String, String> getDimMapper() {
        return dimMapper;
    }

    @JsonProperty
    public void setDimMapper(Map<String, String> dimMapper) {
        this.dimMapper = dimMapper;
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
