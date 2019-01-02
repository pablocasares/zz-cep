package io.wizzie.cep.controllers;

import io.wizzie.cep.model.*;
import io.wizzie.cep.parsers.EventsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SiddhiController {
    private static SiddhiController instance = null;

    private static final Logger log = LoggerFactory.getLogger(SiddhiController.class);
    private String multiId = "";

    ProcessingModel newProcessingModel;

    Map<String, String> streamDefinitions = new HashMap<>();
    Map<String, RuleModel> currentExecutionPlans = new HashMap<>();
    Map<String, SiddhiAppRuntime> executionPlanRuntimes = new HashMap<>();
    KafkaController kafkaController;
    Map<String, Map<String, InputHandler>> inputHandlers = new HashMap<>();
    EventsParser eventsParser = EventsParser.getInstance();
    SiddhiAppBuilder siddhiAppBuilder = new SiddhiAppBuilder();

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

    public static SiddhiController TEST_CreateInstance() {
        return new SiddhiController();
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
            String streamDefinition = siddhiAppBuilder.generateStreamDefinition(streamModel);
            log.debug("Adding stream definition for: " + streamModel.getStreamName());
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

        //Remove the rules that was already added to Siddhi with equal streams section & execution plan before this new config arrived.
        for (Map.Entry<String, RuleModel> executionPlansEntry : currentExecutionPlans.entrySet()) {
            if (expectedExecutionPlans.containsKey(executionPlansEntry.getKey())
                    && (expectedExecutionPlans.get(executionPlansEntry.getKey()).getExecutionPlan().equals(executionPlansEntry.getValue().getExecutionPlan()))
                    && (expectedExecutionPlans.get(executionPlansEntry.getKey()).getStreams().toString().equals(executionPlansEntry.getValue().getStreams().toString()))) {
                expectedExecutionPlans.put(executionPlansEntry.getKey(), executionPlansEntry.getValue());
                log.debug("Rule: " + executionPlansEntry.getKey() + " is already added with the same execution plan and streams section.");
            }
        }

        //Stop the execution plan runtimes that are no longer needed, remove execution plan runtimes and remove input handlers.
        for (Map.Entry<String, RuleModel> executionPlansEntry : currentExecutionPlans.entrySet()) {
            //Delete and stop older rules or delete and stop rules not defined.
            log.debug("Current execution plans: " + currentExecutionPlans);
            log.debug("Expected execution plans: " + expectedExecutionPlans);
            if (executionPlanRuntimes.containsKey(executionPlansEntry.getKey())
                    && !expectedExecutionPlans.containsKey(executionPlansEntry.getKey())) {
                log.debug("Stopping execution plan: " + executionPlansEntry.getKey() + " as it is no longer needed");
                executionPlanRuntimes.get(executionPlansEntry.getKey()).shutdown();
                executionPlanRuntimes.remove(executionPlansEntry.getKey());
                inputHandlers.remove(executionPlansEntry.getKey());
            }

            if (executionPlanRuntimes.containsKey(executionPlansEntry.getKey())
                    && expectedExecutionPlans.containsKey(executionPlansEntry.getKey())
                    && !expectedExecutionPlans.get(executionPlansEntry.getKey()).getExecutionPlan()
                    .equals(executionPlansEntry.getValue().getExecutionPlan())) {
                log.debug("Stopping execution plan: " + executionPlansEntry.getKey() + " as it has a different execution plan");
                executionPlanRuntimes.get(executionPlansEntry.getKey()).shutdown();
                executionPlanRuntimes.remove(executionPlansEntry.getKey());
                inputHandlers.remove(executionPlansEntry.getKey());
            }

            if (executionPlanRuntimes.containsKey(executionPlansEntry.getKey())
                    && expectedExecutionPlans.containsKey(executionPlansEntry.getKey())
                    && !expectedExecutionPlans.get(executionPlansEntry.getKey()).getStreams().toString()
                    .equals(executionPlansEntry.getValue().getStreams().toString())) {
                log.debug("Stopping execution plan: " + executionPlansEntry.getKey() + " as it has a different streams section");
                executionPlanRuntimes.get(executionPlansEntry.getKey()).shutdown();
                executionPlanRuntimes.remove(executionPlansEntry.getKey());
                inputHandlers.remove(executionPlansEntry.getKey());
            }
        }
        log.debug("Stopped the execution plan runtimes that are no longer needed");

        //Now the current execution plans are the expected execution plans.
        currentExecutionPlans.clear();
        currentExecutionPlans.putAll(expectedExecutionPlans);

        log.debug("Creating new execution plan runtimes");
        //Create execution plan runtimes for each new rule, get handlers and add callbacks
        for (Map.Entry<String, RuleModel> executionPlansEntry : expectedExecutionPlans.entrySet()) {
            if (executionPlanRuntimes.containsKey(executionPlansEntry.getKey())) {
                log.debug("Processing app for rule: " + executionPlansEntry.getKey() + " is already created. Skip creating.");
            } else {
                log.debug("Creating new processing app for rule: " + executionPlansEntry.getKey());
                log.debug("Rule: " + executionPlansEntry.getValue().toString());
                StringBuilder fullExecutionPlan = new StringBuilder();

                //Add every stream definition to the new Siddhi Plan
                StreamMapModel streamMapModel = executionPlansEntry.getValue().getStreams();
                for (SourceModel streamModel : streamMapModel.getSourceModel()) {
                    fullExecutionPlan.append(streamDefinitions.get(streamModel.getStreamName())).append(" ");
                }

                //Add rule
                fullExecutionPlan.append(siddhiAppBuilder.generateRuleDefinition(executionPlansEntry.getValue()));

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

                for (SinkModel sinkModel : executionPlansEntry.getValue().getStreams().getSinkModel()) {
                    //Add callbacks. These callbacks send the Siddhi event to KafkaController
                    //This loop checks if an output have been defined at Siddhi. If not, it will log a warning.
                    //It will try to create a relation beetween each rule "out" and Kafka.

                    String streamName = sinkModel.getStreamName();
                    String kafkaTopic = sinkModel.getKafkaTopic();
                    Map<String, String> outputMapper = sinkModel.getDimMapper();

                    StreamDefinition streamDefinition = siddhiAppRuntime.getStreamDefinitionMap().get(streamName);

                    if (streamDefinition != null) {
                        siddhiAppRuntime.addCallback(streamName, new StreamCallback() {
                            @Override
                            public void receive(Event[] events) {
                                for (Event event : events) {
                                    log.debug("Sending event from Siddhi to Kafka");
                                    kafkaController.send2Kafka(kafkaTopic, streamName, event,
                                            siddhiAppRuntime.getStreamDefinitionMap(),
                                            executionPlansEntry.getValue().getOptions(),
                                            outputMapper);
                                }
                            }
                        });
                    } else {
                        log.warn("You specified an output that is not present on the execution plan");
                    }
                }

                log.debug("Starting processing the rule: " + executionPlansEntry.getValue().getId());
                //start processing this rule
                siddhiAppRuntime.start();
            }
        }
    }

    public synchronized void addProcessingModel2KafkaController() {
        if (newProcessingModel != null) {
            if (inputHandlers != null && !inputHandlers.isEmpty()) {
                //Add kafka2sources and input handlers to KafkaController
                log.debug("Adding processing model relations to KafkaController");
                kafkaController.addProcessingModel(newProcessingModel, inputHandlers);
            }
        }
    }

    public void setMultiId(String multiId) {
        this.multiId = multiId;
    }


    public void shutdown() {
        kafkaController.shutdown();
    }

}
