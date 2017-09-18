package io.wizzie.ks.cep.controllers;

import io.wizzie.ks.cep.model.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SiddhiControllerUnitTest {

    @Test
    public void addProcessingDefinitionUnitTest() {
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        SourceModel sourceModel = new SourceModel("streamu1", "inputu1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu1", "outputu1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu1";
        String version = "v1";
        String executionPlan = "from streamu1 select * insert into streamoutputu1";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu1", Arrays.asList(
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
    public void addProcessingDefinitionWithTwoAttributesUnitTest() {
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        SourceModel sourceModel = new SourceModel("streamu2", "inputu2");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu2", "outputu2");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu2";
        String version = "v1";
        String executionPlan = "from streamu2 select * insert into streamoutputu2";

        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu2", Arrays.asList(
                        new AttributeModel("timestamp", "long"),
                        new AttributeModel("value", "string")
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
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        SourceModel sourceModel = new SourceModel("streamu3", "inputu3");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu3", "outputu3");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu3";
        String version = "v1";
        String executionPlan = "from streamu3 select * insert into streamoutputu3";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        String id2 = "ruleu33";
        String version2 = "v1";
        String executionPlan2 = "from streamu3 select * insert into streamoutputu3";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu3", Arrays.asList(
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
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        SourceModel sourceModel = new SourceModel("streamu4", "inputu4");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu4", "outputu4");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu4";
        String version = "v1";
        String executionPlan = "from streamu4 select * insert into streamoutputu4";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        String id2 = "ruleu44";
        String version2 = "v1";
        String executionPlan2 = "from streamu4 select * insert into streamoutputu4";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu4", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("streamu44", Arrays.asList(
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
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        SourceModel sourceModel = new SourceModel("streamu5", "inputu5");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu5", "outputu5");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu5";
        String version = "v1";
        String executionPlan = "from streamu5 select * insert into streamoutputu5";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        String id2 = "ruleu55";
        String version2 = "v1";
        String executionPlan2 = "from streamu5 select * insert into streamoutputu5";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu5", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("streamu55", Arrays.asList(
                        new AttributeModel("attribute2", "long"))));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        id = "ruleu5";
        version = "v1";
        executionPlan = "from streamu5 select * insert into streamoutputu5";

        streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        streamsModel = Arrays.asList(
                new StreamModel("streamu5", Arrays.asList(
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
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        SourceModel sourceModel = new SourceModel("streamu6", "inputu6");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu6", "outputu6");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu6";
        String version = "v1";
        String executionPlan = "from streamu6 select * insert into streamoutputu6";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        sourceModel = new SourceModel("streamu66", "input1");
        sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        sinkModel = new SinkModel("streamoutputu66", "output1");
        sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id2 = "ruleu66";
        String version2 = "v1";
        String executionPlan2 = "from streamu66 select * insert into streamoutputu66";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu6", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("streamu66", Arrays.asList(
                        new AttributeModel("attribute2", "long"))));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        streamsModel = Arrays.asList(
                new StreamModel("streamu6", Arrays.asList(
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
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        //Add Sources and Sinks Definition

        SourceModel sourceModel = new SourceModel("streamnotdefinedu7", "inputu7");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu7", "outputu7");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu7";
        String version = "v1";
        String executionPlan = "from streamnotdefinedu7 select * insert into streamoutputu7";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu7", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("streamu77", Arrays.asList(
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

    @Test
    public void addProcessingDefinitionThenUpdateRuleUnitTest() {
        SiddhiController siddhiController = SiddhiController.TEST_CreateInstance();

        SourceModel sourceModel = new SourceModel("streamu8", "inputu8");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutputu8", "outputu8");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        String id = "ruleu8";
        String version = "v1";
        String executionPlan = "from streamu8 select * insert into streamoutputu8";

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);


        String id2 = "ruleu88";
        String version2 = "v1";
        String executionPlan2 = "from streamu8 select * insert into streamoutputu8";

        StreamMapModel streamMapModel2 = new StreamMapModel(sourceModelList, sinkModelList);

        RuleModel ruleModelObject2 = new RuleModel(id2, version2, streamMapModel2, executionPlan2, null);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);
        ruleModelList.add(ruleModelObject2);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamu8", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )), new StreamModel("streamu88", Arrays.asList(
                        new AttributeModel("attribute2", "long"))));

        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        id = "ruleu8";
        version = "v2";
        executionPlan = "from streamu8 select * insert into streamoutputu8";

        streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan, null);

        ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        streamsModel = Arrays.asList(
                new StreamModel("streamu8", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
        siddhiController.generateExecutionPlans();

        assertEquals(1, siddhiController.inputHandlers.size());
        assertEquals(1, siddhiController.executionPlanRuntimes.size());
        assertEquals(1, siddhiController.streamDefinitions.size());
        assertEquals(1, siddhiController.currentExecutionPlans.size());
        assertEquals("v2", siddhiController.currentExecutionPlans.get("ruleu8").getVersion());

    }


}
