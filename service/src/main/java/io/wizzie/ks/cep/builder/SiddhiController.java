package io.wizzie.ks.cep.builder;

import io.wizzie.ks.cep.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.Map;

public class SiddhiController {
    private static SiddhiController instance = null;

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);

    Map<String, StreamModel> streamsDefinition = new HashMap<>();
    Map<String, SourceModel> sourcesDefinition = new HashMap<>();
    Map<String, SinkModel> sinksDefinition = new HashMap<>();
    Map<String, RuleModel> rulesDefinition = new HashMap<>();
    Map<String, String> currentExecutionPlans = new HashMap<>();
    Map<String, SiddhiAppRuntime> executionPlanRuntimes = new HashMap<>();
    KafkaController kafkaController;
    InOutStreamModel currentInOutStreamModel;
    Map<String, InputHandler> inputHandlers = new HashMap<>();
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


    public void addStreamDefinition(InOutStreamModel inOutStreamModel) {
        streamsDefinition.clear();
        for (StreamModel streamModel : inOutStreamModel.getStreams()) {
            streamsDefinition.put(streamModel.getStreamName(), streamModel);
        }
        this.currentInOutStreamModel = inOutStreamModel;
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

        //Create every streamDefinition, save handlers and create eventParsers
        eventsParser.clear();
        for (Map.Entry<String, StreamModel> streamEntry : streamsDefinition.entrySet()) {

            //create stream definition
            StreamDefinition streamDefinition = generateStreamDefinition(streamEntry.getValue().getStreamName(), streamEntry.getValue());
            SiddhiApp siddhiApp = new SiddhiApp(streamEntry.getKey());
            siddhiApp.defineStream(StreamDefinition.id(streamEntry.getKey())).defineStream(streamDefinition);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

            //save handler
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler(streamEntry.getValue().getStreamName());
            inputHandlers.put(streamEntry.getKey(), inputHandler);

            //add events format to eventsParser
            eventsParser.addEventFormat(streamEntry.getKey(), streamEntry.getValue());
        }

        //Create every ruleDefinition checking if streamDefinition exists
        for (Map.Entry<String, RuleModel> ruleEntry : rulesDefinition.entrySet()) {
            for (String stream : ruleEntry.getValue().getStreams()) {
                if (streamsDefinition.containsKey(stream)) {
                    expectedExecutionPlans.put(ruleEntry.getKey(), generateRuleDefinition(ruleEntry.getValue()));
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
                currentExecutionPlans.remove(executionPlansEntry.getKey());
                siddhiManager.getSiddhiAppRuntime(executionPlansEntry.getKey()).shutdown();
                log.debug("Stopping execution plan: " + executionPlansEntry.getKey() + " as it is no longer needed");
            }
        }


        //Now the expected execution plans are the current execution plans + expected execution plans.
        currentExecutionPlans.putAll(expectedExecutionPlans);

        //Create execution plan runtime for each rule and get handlers
        for (Map.Entry<String, String> executionPlansEntry : currentExecutionPlans.entrySet()) {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(executionPlansEntry.getValue());
            executionPlanRuntimes.put(executionPlansEntry.getKey(), siddhiAppRuntime);
            //start processing this rule
            siddhiAppRuntime.start();

            //Add callback. This callback sends the Siddhi event to SiddhiController
            siddhiAppRuntime.addCallback(executionPlansEntry.getKey(), new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    for (Event event : inEvents) {
                        kafkaController.send2Kafka(executionPlansEntry.getKey(), event);
                    }
                }
            });
        }

        //Add kafka2sources and input handlers to KafkaController
        kafkaController.addSources2Stream(currentInOutStreamModel, inputHandlers);
        //Add Siddhi2kafka
        kafkaController.addStream2Sinks(currentInOutStreamModel, rulesDefinition);

    }


    private StreamDefinition generateStreamDefinition(String streamName, StreamModel streamModel) {

        StreamDefinition streamDefinition = StreamDefinition.id(streamName);
        for (AttributeModel attributeModel : streamModel.getAttributes()) {
            switch (attributeModel.getAttributeType()) {
                case "string":
                    streamDefinition.attribute(attributeModel.getName(), Attribute.Type.STRING);
                    break;
                case "int":
                    streamDefinition.attribute(attributeModel.getName(), Attribute.Type.INT);
                    break;
                case "long":
                    streamDefinition.attribute(attributeModel.getName(), Attribute.Type.LONG);
                    break;
                case "float":
                    streamDefinition.attribute(attributeModel.getName(), Attribute.Type.FLOAT);
                    break;
                case "bool":
                    streamDefinition.attribute(attributeModel.getName(), Attribute.Type.BOOL);
                    break;
                case "object":
                    streamDefinition.attribute(attributeModel.getName(), Attribute.Type.OBJECT);
                    break;
                case "double":
                    streamDefinition.attribute(attributeModel.getName(), Attribute.Type.DOUBLE);
                    break;
            }
        }


        return streamDefinition;
    }

    private String generateRuleDefinition(RuleModel ruleModel) {

        StringBuilder ruleDefinition = new StringBuilder();
        ruleDefinition.append("@info(name = '" + ruleModel.getId() + "') ");
        ruleDefinition.append(ruleModel.getExecutionPlan());
        ruleDefinition.append(" ;");

        return ruleDefinition.toString();
    }


}
