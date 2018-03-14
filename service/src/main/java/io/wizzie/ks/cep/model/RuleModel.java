package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RuleModel {
    Integer id;
    String version;
    List<String> streams;
    String executionPlan;

    @JsonCreator
    public RuleModel(@JsonProperty("id") Integer id,
                     @JsonProperty("version") String version,
                     @JsonProperty("streams") List<String> streams,
                     @JsonProperty("executionPlan") String executionPlan) {
        this.id = id;
        this.version = version;
        this.streams = streams;
        this.executionPlan = executionPlan;
    }

    @JsonProperty
    public Integer getId() {
        return id;
    }

    @JsonProperty
    public void setId(Integer id) {
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
    public List<String> getStreams() {
        return streams;
    }

    @JsonProperty
    public void setStreams(List<String> streams) {
        this.streams = streams;
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
                .append("streams: ").append(streams).append(", ")
                .append("executionPlan: ").append(executionPlan)
                .append("}");

        return sb.toString();
    }
}
