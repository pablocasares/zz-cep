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

    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public List<AttributeModel> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<AttributeModel> attributes) {
        this.attributes = attributes;
    }
}
