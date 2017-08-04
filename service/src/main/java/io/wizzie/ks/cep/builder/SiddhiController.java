package io.wizzie.ks.cep.builder;

import io.wizzie.ks.cep.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.HashMap;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SiddhiController {

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    Map<String, StreamModel> streamsDefinition = new HashMap<>();
    Map<String, SourceModel> sourcesDefinition = new HashMap<>();
    Map<String, SinkModel> sinksDefinition = new HashMap<>();
    Map<String, RuleModel> rulesDefinition = new HashMap<>();
    Map<String, String> currentExecutionPlans = new HashMap<>();
    Map<String, ExecutionPlanRuntime> executionPlanRuntimes = new HashMap<>();

    SiddhiManager siddhiManager;

    public SiddhiController() {
        siddhiManager = new SiddhiManager();
    }

    public void addStreamDefinition(InOutStreamModel inOutStreamModel) {
        for (StreamModel streamModel : inOutStreamModel.getStreams()) {
            streamsDefinition.put(streamModel.getStreamName(), streamModel);
        }
    }

    public void addRulesDefinition(ProcessingModel processingModel) {
        for (RuleModel ruleModel : processingModel.getRules()) {
            if (!(rulesDefinition.containsKey(ruleModel.getId())) || !(rulesDefinition.get(ruleModel.getId()).equals(ruleModel.getId()))) {
                rulesDefinition.put(ruleModel.getId(), ruleModel);
            } else {
                log.warn("Rule: " + ruleModel.getId() + " already exists. You should set a new ID or change version if you want add it.");
            }
        }
    }

    public void generateExecutionPlans() {

        Map<String, String> expectedExecutionPlans = new HashMap<>();

        //Create every streamDefinition+ruleDefinition checking if streamDefinition exists
        for (Map.Entry<String, RuleModel> ruleEntry : rulesDefinition.entrySet()) {
            for (String stream : ruleEntry.getValue().getStreams()) {
                if (streamsDefinition.containsKey(stream)) {
                    expectedExecutionPlans.put(ruleEntry.getKey(), generateStreamDefinition(streamsDefinition.get(stream)) + generateRuleDefinition(ruleEntry.getValue()));
                } else {
                    log.warn("The rule: " + ruleEntry.getKey() + " doesn't have the stream definition: " + stream + " associated with it. You need to define it.");
                }
            }
        }

        //Remove the rules that was already added to Siddhi before this new config arrived.
        for (Map.Entry<String, String> executionPlansEntry : currentExecutionPlans.entrySet()) {
            if (expectedExecutionPlans.containsKey(executionPlansEntry.getKey()))
                expectedExecutionPlans.remove(executionPlansEntry.getKey());
        }

        //Stop the execution plan runtimes that are no longer needed
        for (Map.Entry<String, String> executionPlansEntry : currentExecutionPlans.entrySet()) {
            if (!expectedExecutionPlans.containsKey(executionPlansEntry.getKey())) {
                executionPlanRuntimes.get(executionPlansEntry.getKey()).shutdown();
                log.debug("Stopping execution plan: " + executionPlansEntry.getKey() + " as it is no longer needed");
            }
        }


        //Now the expected execution plans are the current execution plans.
        currentExecutionPlans.clear();
        currentExecutionPlans.putAll(expectedExecutionPlans);

        //Create execution plan runtime for each rule
        for (Map.Entry<String, String> executionPlansEntry : currentExecutionPlans.entrySet()) {
            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlansEntry.getValue());
            executionPlanRuntimes.put(executionPlansEntry.getKey(), executionPlanRuntime);
        }


    }


    private String generateStreamDefinition(StreamModel streamModel) {

        StringBuilder streamDefinition = new StringBuilder();
        streamDefinition.append("@config(async = 'true') define stream ");
        streamDefinition.append(streamModel.getStreamName());
        streamDefinition.append(" (");
        for (AttributeModel attributeModel : streamModel.getAttributes()) {
            streamDefinition.append(attributeModel.getName() + " " + attributeModel.getAttributeType());
        }
        streamDefinition.append(");");

        return streamDefinition.toString();
    }

    private String generateRuleDefinition(RuleModel ruleModel) {

        StringBuilder ruleDefinition = new StringBuilder();
        ruleDefinition.append("@info(name = '" + ruleModel.getId() + "') ";
        ruleDefinition.append(ruleModel.getExecutionPlan());
        ruleDefinition.append(" ;");

        return ruleDefinition.toString();
    }


}
