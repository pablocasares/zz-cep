package io.wizzie.cep.model;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.HashMap;
import java.util.Map;

public class SiddhiAppBuilder {

    private static final Logger log = LoggerFactory.getLogger(SiddhiAppBuilder.class);

    public String generateStreamDefinition(StreamModel streamModel) {

        StringBuilder streamDefinition = new StringBuilder();
        streamDefinition.append("@config(async = 'true') define stream ");
        streamDefinition.append(streamModel.getStreamName());
        streamDefinition.append(" (");
        int i = 0;
        for (AttributeModel attributeModel : streamModel.getAttributes()) {
            i++;
            streamDefinition.append(attributeModel.getName() + " " + attributeModel.getAttributeType());
            if (i != streamModel.getAttributes().size()) {
                streamDefinition.append(", ");
            }
        }
        streamDefinition.append(");");

        return streamDefinition.toString();
    }

    public String generateRuleDefinition(RuleModel ruleModel) {

        StringBuilder ruleDefinition = new StringBuilder();
        ruleDefinition.append("@info(name = '" + ruleModel.getId() + "') ");
        ruleDefinition.append(ruleModel.getExecutionPlan());
        ruleDefinition.append(" ;");

        return ruleDefinition.toString();
    }


    public boolean validateSiddhiPlan(ProcessingModel processingModel) {

        boolean isValid = false;

        Map<String, Object> streamDefinitions = new HashMap<>();
        Map<String, RuleModel> expectedExecutionPlans = new HashMap<>();

        for (StreamModel streamModel : processingModel.getStreams()) {
            //create stream definition
            String streamDefinition = generateStreamDefinition(streamModel);
            streamDefinitions.put(streamModel.getStreamName(), streamDefinition);
        }

        for (RuleModel rule : processingModel.getRules()) {
            StreamMapModel streamMapModel = rule.getStreams();
            for (SourceModel sourceModel : streamMapModel.getSourceModel()) {
                if (streamDefinitions.containsKey(sourceModel.getStreamName())) {
                    expectedExecutionPlans.put(rule.getId(), rule);
                } else {
                    log.warn("The rule: " + rule.getId() + " doesn't have the stream definition: " + sourceModel.getStreamName() + " associated with it. You need to define it.");
                }
            }
        }

        for (Map.Entry<String, RuleModel> executionPlansEntry : expectedExecutionPlans.entrySet()) {

            log.debug("Creating new processing app for rule: " + executionPlansEntry.getKey());
            log.debug("Rule: " + executionPlansEntry.getValue().toString());
            StringBuilder fullExecutionPlan = new StringBuilder();

            //Add every stream definition to the new Siddhi Plan
            StreamMapModel streamMapModel = executionPlansEntry.getValue().getStreams();
            for (SourceModel streamModel : streamMapModel.getSourceModel()) {
                fullExecutionPlan.append(streamDefinitions.get(streamModel.getStreamName())).append(" ");
            }

            //Add rule
            fullExecutionPlan.append(generateRuleDefinition(executionPlansEntry.getValue()));

            SiddhiManager siddhiManager = new SiddhiManager();
            //Create runtime
            siddhiManager.validateSiddhiApp(fullExecutionPlan.toString());
            isValid = true;
        }
        return isValid;
    }
}
