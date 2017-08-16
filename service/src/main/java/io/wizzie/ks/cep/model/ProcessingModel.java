package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ProcessingModel {
    List<RuleModel> rules;
    List<StreamModel> streams;

    @JsonCreator
    public ProcessingModel(@JsonProperty("rules") List<RuleModel> rules, @JsonProperty("streams") List<StreamModel> streams) {
        this.rules = rules;
        this.streams = streams;
    }

    @JsonProperty
    public List<RuleModel> getRules() {
        return rules;
    }

    @JsonProperty
    public void setRules(List<RuleModel> rules) {
        this.rules = rules;
    }

    @JsonProperty
    public List<StreamModel> getStreams() {
        return streams;
    }
    @JsonProperty
    public void setStreams(List<StreamModel> streams) {
        this.streams = streams;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("streams: ").append(streams)
                .append(", rules: ").append(rules)
                .append("}\n");

        return sb.toString();
    }
}

