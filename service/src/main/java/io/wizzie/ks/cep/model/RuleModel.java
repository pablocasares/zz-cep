package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class RuleModel {
    String id;
    String version;
    StreamMap streamMap;
    String executionPlan;

    @JsonCreator
    public RuleModel(@JsonProperty("id") String id,
                     @JsonProperty("version") String version,
                     @JsonProperty("streamMap") StreamMap streams,
                     @JsonProperty("executionPlan") String executionPlan) {
        this.id = id;
        this.version = version;
        this.streamMap = streams;
        this.executionPlan = executionPlan;
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
    public String getVersion() {
        return version;
    }

    @JsonProperty
    public void setVersion(String version) {
        this.version = version;
    }

    @JsonProperty
    public StreamMap getStreams() {
        return streamMap;
    }

    @JsonProperty
    public void setStreams(StreamMap streams) {
        this.streamMap = streams;
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
        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("id: ").append(id).append(", ")
                .append("version: ").append(version).append(", ")
                .append("streams: ").append(streamMap).append(", ")
                .append("executionPlan: ").append(executionPlan)
                .append("}");

        return sb.toString();
    }
}
