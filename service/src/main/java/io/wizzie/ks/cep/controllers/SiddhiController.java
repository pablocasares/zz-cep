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

public class SiddhiController {
    private static SiddhiController instance = null;

    private static final Logger log = LoggerFactory.getLogger(SiddhiController.class);

    Map<String, StreamModel> streamsDefinitionModels = new HashMap<>();
    Map<String, String> streamDefinitions = new HashMap<>();
    Map<String, RuleModel> rulesDefinition = new HashMap<>();
    Map<String, RuleModel> currentExecutionPlans = new HashMap<>();
    Map<String, SiddhiAppRuntime> executionPlanRuntimes = new HashMap<>();
    KafkaController kafkaController;
    InOutStreamModel currentInOutStreamModel;
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

    public void initKafkaController(String kafkaCluster){
        this.kafkaController.init(kafkaCluster);
    }

    public void addStreamDefinition(InOutStreamModel inOutStreamModel) {
        log.debug("Adding Streams definition.");
        streamsDefinitionModels.clear();
        for (StreamModel streamModel : inOutStreamModel.getStreams()) {
            streamsDefinitionModels.put(streamModel.getStreamName(), streamModel);
        }
        this.currentInOutStreamModel = inOutStreamModel;
    }

    public void addRulesDefinition(ProcessingModel processingModel) {
        log.debug("Adding Rules Defintions.");
        for (RuleModel ruleModel : processingModel.getRules()) {
            if (!(rulesDefinition.containsKey(ruleModel.getId())) || !(rulesDefinition.get(ruleModel.getId()).equals(ruleModel.getId()))) {
                rulesDefinition.put(ruleModel.getId(), ruleModel);
            } else {
                log.warn("Rule: " + ruleModel.getId() + " already exists. You should set a new ID or change version if you want add it.");
            }
        }
    }

    public void generateExecutionPlans() {

        log.debug("Generating execution plans.");
        Map<String, RuleModel> expectedExecutionPlans = new HashMap<>();


        //Create every streamDefinition and create eventParsers
        eventsParser.clear();
        streamDefinitions.clear();
        for (Map.Entry<String, StreamModel> streamEntry : streamsDefinitionModels.entrySet()) {
            //create stream definition
            String streamDefinition = generateStreamDefinition(streamEntry.getValue());
            streamDefinitions.put(streamEntry.getKey(), streamDefinition);

            //add events format to eventsParser
            eventsParser.addEventFormat(streamEntry.getKey(), streamEntry.getValue());
        }
        log.debug("Created every streamDefinition and created eventParsers");

        //Create every ruleDefinition checking if streamDefinition exists
        for (Map.Entry<String, RuleModel> ruleEntry : rulesDefinition.entrySet()) {
            for (String stream : ruleEntry.getValue().getStreams()) {
                if (streamsDefinitionModels.containsKey(stream)) {
                    expectedExecutionPlans.put(ruleEntry.getKey(), ruleEntry.getValue());
                } else {
                    log.warn("The rule: " + ruleEntry.getKey() + " doesn't have the stream definition: " + stream + " associated with it. You need to define it.");
                }
            }
        }
        log.debug("Created every ruleDefinition checking if streamDefinition exists");

        //Remove the rules that was already added to Siddhi with an equal version before this new config arrived.
        for (Map.Entry<String, RuleModel> executionPlansEntry : currentExecutionPlans.entrySet()) {
            if (expectedExecutionPlans.containsKey(executionPlansEntry.getKey()) && (expectedExecutionPlans.get(executionPlansEntry.getKey()).getVersion().equals(executionPlansEntry.getValue().getVersion())))
                expectedExecutionPlans.remove(executionPlansEntry.getKey());
        }

        //Stop the execution plan runtimes that are no longer needed
        for (Map.Entry<String, RuleModel> executionPlansEntry : currentExecutionPlans.entrySet()) {
            //Delete and stop older rules or delete and stop rules not defined.
            if (expectedExecutionPlans.containsKey(executionPlansEntry.getKey()) || !rulesDefinition.containsKey(executionPlansEntry.getKey())) {
                currentExecutionPlans.remove(executionPlansEntry.getKey());
                siddhiManager.getSiddhiAppRuntime(executionPlansEntry.getKey()).shutdown();
                log.debug("Stopping execution plan: " + executionPlansEntry.getKey() + " as it is no longer needed");
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
            for (String streamModel : executionPlansEntry.getValue().getStreams()) {
                fullExecutionPlan.append(streamDefinitions.get(streamModel)).append(" ");
            }

            //Add rule
            fullExecutionPlan.append(generateRuleDefinition(executionPlansEntry.getValue()));

            //Create runtime
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(fullExecutionPlan.toString());
            executionPlanRuntimes.put(executionPlansEntry.getKey(), siddhiAppRuntime);


            //save handlers for this rule
            for (String streamModel : executionPlansEntry.getValue().getStreams()) {
                InputHandler inputHandler = siddhiAppRuntime.getInputHandler(streamModel);

                //Save handler at inputHandlers Map. Map format: ("rule1" --> ("stream1"-->inputHandler))
                inputHandlers.putIfAbsent(executionPlansEntry.getKey(), new HashMap<>());
                Map<String, InputHandler> inputHandlerMap = inputHandlers.get(executionPlansEntry.getKey());
                inputHandlerMap.put(streamModel, inputHandler);
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

            log.debug("Starting processing the rule: "+ executionPlansEntry.getValue().getId());
            //start processing this rule
            siddhiAppRuntime.start();
        }

        if (currentInOutStreamModel != null) {
            if (inputHandlers != null) {
                //Add kafka2sources and input handlers to KafkaController
                log.debug("Adding sources2streams relations to KafkaController");
                kafkaController.addSources2Stream(currentInOutStreamModel, inputHandlers);
            }
            if (rulesDefinition != null && !rulesDefinition.isEmpty()) {
                //Add Siddhi2kafka
                log.debug("Adding streams2sinks relations to KafkaController");
                log.debug("Rules definitions: "+rulesDefinition);
                kafkaController.addStream2Sinks(currentInOutStreamModel, rulesDefinition);
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
