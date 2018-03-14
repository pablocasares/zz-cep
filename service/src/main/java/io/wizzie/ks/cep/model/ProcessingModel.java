package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ProcessingModel {
    List<RuleModel> rules;

    @JsonCreator
    public ProcessingModel(@JsonProperty("rules") List<RuleModel> rules) {
        this.rules = rules;
    }

    @JsonProperty
    public List<RuleModel> getRules() {
        return rules;
    }

    @JsonProperty
    public void setRules(List<RuleModel> rules) {
        this.rules = rules;
    }
}

