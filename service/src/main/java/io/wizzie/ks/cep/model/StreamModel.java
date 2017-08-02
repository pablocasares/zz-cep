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
}
