package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RuleModel {
    String id;
    String version;
    StreamMapModel streamMapModel;
    String executionPlan;

    @JsonCreator
    public RuleModel(@JsonProperty("id") String id,
                     @JsonProperty("version") String version,
                     @JsonProperty("streamMapModel") StreamMapModel streams,
                     @JsonProperty("executionPlan") String executionPlan) {
        this.id = id;
        this.version = version;
        this.streamMapModel = streams;
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
        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("id: ").append(id).append(", ")
                .append("version: ").append(version).append(", ")
                .append("streams: ").append(streamMapModel).append(", ")
                .append("executionPlan: ").append(executionPlan)
                .append("}");

        return sb.toString();
    }
}
