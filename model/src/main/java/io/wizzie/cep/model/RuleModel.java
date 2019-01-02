package io.wizzie.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class RuleModel {
    String id;
    StreamMapModel streamMapModel;
    String executionPlan;
    Map<String, Object> options;

    @JsonCreator
    public RuleModel(@JsonProperty("id") String id,
                     @JsonProperty("streamMapModel") StreamMapModel streams,
                     @JsonProperty("executionPlan") String executionPlan,
                     @JsonProperty("options") Map<String, Object> options) {
        this.id = id;
        this.streamMapModel = streams;
        this.executionPlan = executionPlan;
        this.options = options;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    @JsonProperty
    public String getId() {
        return id;
    }

    @JsonProperty
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty
    public StreamMapModel getStreams() {
        return streamMapModel;
    }

    @JsonProperty
    public void setStreams(StreamMapModel streams) {
        this.streamMapModel = streams;
    }

    @JsonProperty
    public String getExecutionPlan() {
        return executionPlan;
    }

    @JsonProperty
    public void setExecutionPlan(String executionPlan) {
        this.executionPlan = executionPlan;
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();

        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("id: ").append(id).append(", ")
                .append("streams: ").append(streamMapModel).append(", ")
                .append("executionPlan: ").append(executionPlan).append(", ")
                .append("options: ").append(options)
                .append("}");

        return sb.toString();
    }
}
