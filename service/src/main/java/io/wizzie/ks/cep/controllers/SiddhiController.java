package io.wizzie.ks.cep.controllers;

import io.wizzie.ks.cep.model.*;
import io.wizzie.ks.cep.parsers.EventsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.SiddhiApp;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SiddhiController {
    private static SiddhiController instance = null;

    private static final Logger log = LoggerFactory.getLogger(SiddhiController.class);


    ProcessingModel newProcessingModel;

    Map<String, String> streamDefinitions = new HashMap<>();
    Map<String, RuleModel> rulesDefinition = new HashMap<>();
    Map<String, RuleModel> currentExecutionPlans = new HashMap<>();
    Map<String, SiddhiAppRuntime> executionPlanRuntimes = new HashMap<>();
    KafkaController kafkaController;
    Map<String, Map<String, InputHandler>> inputHandlers = new HashMap<>();
    EventsParser eventsParser = EventsParser.getInstance();

    SiddhiManager siddhiManager;

    private SiddhiController() {
        this.siddhiManager = new SiddhiManager();
        this.kafkaController = new KafkaController();
    }

    public static SiddhiController getInstance() {
        if (instance == null) {
            instance = new SiddhiController();
        }
        return instance;
    }

    public void initKafkaController(Properties consumerProperties, Properties producerProperties) {
        this.kafkaController.init(consumerProperties, producerProperties);
    }

    public void addProcessingDefinition(ProcessingModel processingModel) {
        log.debug("Adding new processing model definition.");
        this.newProcessingModel = processingModel;
    }

    public synchronized void generateExecutionPlans() {

        log.debug("Generating execution plans.");
        Map<String, RuleModel> expectedExecutionPlans = new HashMap<>();


        //Create every streamDefinition and create eventParsers for each stream.
        eventsParser.clear();
        streamDefinitions.clear();
        for (StreamModel streamModel : newProcessingModel.getStreams()) {
            //create stream definition
            String streamDefinition = generateStreamDefinition(streamModel);
            streamDefinitions.put(streamModel.getStreamName(), streamDefinition);

            //add events format to eventsParser
            eventsParser.addEventFormat(streamModel.getStreamName(), streamModel);
        }
        log.debug("Created every streamDefinition and created eventParsers");

        //Create every ruleDefinition checking if streamDefinition exists
        for (RuleModel rule : newProcessingModel.getRules()) {
            StreamMapModel streamMapModel = rule.getStreams();
            for (SourceModel sourceModel : streamMapModel.getSourceModel()) {
                if (streamDefinitions.containsKey(sourceModel.getStreamName())) {
                    expectedExecutionPlans.put(rule.getId(), rule);
                } else {
                    log.warn("The rule: " + rule.getId() + " doesn't have the stream definition: " + sourceModel.getStreamName() + " associated with it. You need to define it.");
                }
            }
        }
        log.debug("Created every ruleDefinition checking if streamDefinition exists");

        //Remove the rules that was already added to Siddhi with an equal version before this new config arrived.
        for (Map.Entry<String, RuleModel> executionPlansEntry : currentExecutionPlans.entrySet()) {
            if (expectedExecutionPlans.containsKey(executionPlansEntry.getKey()) && (expectedExecutionPlans.get(executionPlansEntry.getKey()).getVersion().equals(executionPlansEntry.getValue().getVersion()))) {
                expectedExecutionPlans.remove(executionPlansEntry.getKey());
                log.debug("Rule: " + executionPlansEntry.getKey() + " is already added. Removing it from expected execution plans.");
            }
        }

        // TODO: FIX THIS LOOP: Stop the execution plan runtimes that are no longer needed

        for (Map.Entry<String, RuleModel> executionPlansEntry : currentExecutionPlans.entrySet()) {
            //Delete and stop older rules or delete and stop rules not defined.
            log.debug("Current execution plans: " + currentExecutionPlans);
            log.debug("Expected new execution plans: " + expectedExecutionPlans);
            log.debug("Execution plans after generating: " + newProcessingModel.getRules());
            if (!expectedExecutionPlans.containsKey(executionPlansEntry.getKey())) { //|| !rulesDefinition.containsKey(executionPlansEntry.getKey())) {
                currentExecutionPlans.remove(executionPlansEntry.getKey());
                log.debug("Stopping execution plan: " + executionPlansEntry.getKey() + " as it is no longer needed");
                siddhiManager.getSiddhiAppRuntime(executionPlansEntry.getKey()).shutdown();
            }
        }
        log.debug("Stopped the execution plan runtimes that are no longer needed");

        //Now the current execution plans are the current execution plans + newer expected execution plans.
        currentExecutionPlans.putAll(expectedExecutionPlans);

        log.debug("Creating execution plan runtimes");
        //Create execution plan runtimes for each new rule, get handlers and add callbacks
        for (Map.Entry<String, RuleModel> executionPlansEntry : expectedExecutionPlans.entrySet()) {

            StringBuilder fullExecutionPlan = new StringBuilder();

            //Create a SiddhiApp for each rule
            SiddhiApp siddhiApp = new SiddhiApp(executionPlansEntry.getKey());

            //Add every stream definition to the new Siddhi Plan
            StreamMapModel streamMapModel = executionPlansEntry.getValue().getStreams();
            for (SourceModel streamModel : streamMapModel.getSourceModel()) {
                fullExecutionPlan.append(streamDefinitions.get(streamModel.getStreamName())).append(" ");
            }

            //Add rule
            fullExecutionPlan.append(generateRuleDefinition(executionPlansEntry.getValue()));

            //Create runtime
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(fullExecutionPlan.toString());
            executionPlanRuntimes.put(executionPlansEntry.getKey(), siddhiAppRuntime);


            //save handlers for this rule
            StreamMapModel streamMapModel1 = executionPlansEntry.getValue().getStreams();
            for (SourceModel streamModel : streamMapModel1.getSourceModel()) {
                InputHandler inputHandler = siddhiAppRuntime.getInputHandler(streamModel.getStreamName());

                //Save handler at inputHandlers Map. Map format: ("rule1" --> ("stream1"-->inputHandler))
                inputHandlers.putIfAbsent(executionPlansEntry.getKey(), new HashMap<>());
                Map<String, InputHandler> inputHandlerMap = inputHandlers.get(executionPlansEntry.getKey());
                inputHandlerMap.put(streamModel.getStreamName(), inputHandler);
                inputHandlers.put(executionPlansEntry.getKey(), inputHandlerMap);
            }
            //Add callback. This callback sends the Siddhi event to SiddhiController
            siddhiAppRuntime.addCallback(executionPlansEntry.getKey(), new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    for (Event event : inEvents) {
                        log.debug("Sending event from Siddhi to Kafka");
                        kafkaController.send2Kafka(executionPlansEntry.getKey(), event);
                    }
                }
            });

            log.debug("Starting processing the rule: " + executionPlansEntry.getValue().getId());
            //start processing this rule
            siddhiAppRuntime.start();
        }

        if (newProcessingModel != null) {
            if (inputHandlers != null) {
                //Add kafka2sources and input handlers to KafkaController
                log.debug("Adding processing model relations to KafkaController");
                kafkaController.addProcessingModel(newProcessingModel, inputHandlers);
            }
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
        ruleDefinition.append("@info(name = '" + ruleModel.getId() + "') ");
        ruleDefinition.append(ruleModel.getExecutionPlan());
        ruleDefinition.append(" ;");

        return ruleDefinition.toString();
    }


}
