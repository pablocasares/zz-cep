package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StreamModel {
    String streamName;
    List<AttributeModel> attributes;

    @JsonCreator
    public StreamModel(@JsonProperty("streamName") String streamName,
                       @JsonProperty("attributes") List<AttributeModel> attributes) {
        this.streamName = streamName;
        this.attributes = attributes;
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
    public List<AttributeModel> getAttributes() {
        return attributes;
    }

    @JsonProperty
    public void setAttributes(List<AttributeModel> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("streamName: ").append(streamName).append(", ")
                .append("attributes: ").append(attributes)
                .append("}");

        return sb.toString();
    }
}
