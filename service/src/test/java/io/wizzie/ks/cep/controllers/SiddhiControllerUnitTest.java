package io.wizzie.ks.cep.controllers;

import io.wizzie.ks.cep.model.*;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SiddhiControllerUnitTest {

    @Test
    public void addProcessingDefinitionUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule1";
        String version = "v1";
        String executionPlan = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        assertEquals(1, siddhiController.inputHandlers.size());
        assertEquals(1, siddhiController.executionPlanRuntimes.size());
        assertEquals(1, siddhiController.streamDefinitions.size());
        assertEquals(1, siddhiController.currentExecutionPlans.size());

    }

    @Test
    public void addProcessingDefinitionWithTwoRulesUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule2";
        String version = "v1";
        String executionPlan = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);


        String id2 = "rule3";
        String version2 = "v1";
        String executionPlan2 = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        assertEquals(2, siddhiController.inputHandlers.size());
        assertEquals(2, siddhiController.executionPlanRuntimes.size());
        assertEquals(1, siddhiController.streamDefinitions.size());
        assertEquals(2, siddhiController.currentExecutionPlans.size());

    }


    @Test
    public void addProcessingDefinitionWithTwoRulesAndTwoStreamsUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule2";
        String version = "v1";
        String executionPlan = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);


        String id2 = "rule3";
        String version2 = "v1";
        String executionPlan2 = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("stream2", Arrays.asList(
                        new AttributeModel("attribute2", "long"))));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        assertEquals(2, siddhiController.inputHandlers.size());
        assertEquals(2, siddhiController.executionPlanRuntimes.size());
        assertEquals(2, siddhiController.streamDefinitions.size());
        assertEquals(2, siddhiController.currentExecutionPlans.size());

    }

    @Test
    public void addProcessingDefinitionThenDeleteRuleUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule2";
        String version = "v1";
        String executionPlan = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);


        String id2 = "rule3";
        String version2 = "v1";
        String executionPlan2 = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("stream2", Arrays.asList(
                        new AttributeModel("attribute2", "long"))));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        id = "rule1";
        version = "v1";
        executionPlan = "from stream select * insert into streamoutput";

        streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        streamsModel = Arrays.asList(
                new StreamModel("stream", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        assertEquals(1, siddhiController.inputHandlers.size());
        assertEquals(1, siddhiController.executionPlanRuntimes.size());
        assertEquals(1, siddhiController.streamDefinitions.size());
        assertEquals(1, siddhiController.currentExecutionPlans.size());

    }


    @Test
    public void addProcessingDefinitionThenDeleteStreamUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule2";
        String version = "v1";
        String executionPlan = "from stream select * insert into streamoutput";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);


        sourceModel = new SourceModel("stream2", "input1");
        sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        sinkModel = new SinkModel("streamoutput2", "output1");
        sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id2 = "rule3";
        String version2 = "v1";
        String executionPlan2 = "from stream2 select * insert into streamoutput";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("stream2", Arrays.asList(
                        new AttributeModel("attribute2", "long"))));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        streamsModel = Arrays.asList(
                new StreamModel("stream", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        assertEquals(1, siddhiController.inputHandlers.size());
        assertEquals(1, siddhiController.executionPlanRuntimes.size());
        assertEquals(1, siddhiController.streamDefinitions.size());
        assertEquals(1, siddhiController.currentExecutionPlans.size());

    }


    @Test
    public void addProcessingDefinitionWithNoStreamAssociatedUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        //Add Sources and Sinks Definition

        SourceModel sourceModel = new SourceModel("streamnotdefined", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "rule2";
        String version = "v1";
        String executionPlan = "from streamnotdefined select * insert into streamoutput";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream3", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("stream2", Arrays.asList(
                        new AttributeModel("attribute2", "long"))));


        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        assertEquals(0, siddhiController.inputHandlers.size());
        assertEquals(0, siddhiController.executionPlanRuntimes.size());
        assertEquals(2, siddhiController.streamDefinitions.size());
        assertEquals(0, siddhiController.currentExecutionPlans.size());

    }

}
