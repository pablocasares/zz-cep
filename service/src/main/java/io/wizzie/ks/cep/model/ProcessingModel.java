package io.wizzie.ks.cep.model;

import java.util.List;

public class ProcessingModel {
    List<RuleModel> rules;

    public ProcessingModel(List<RuleModel> rules) {
        this.rules = rules;
    }

    public List<RuleModel> getRules() {
        return rules;
    }

    public void setRules(List<RuleModel> rules) {
        this.rules = rules;
    }
}

