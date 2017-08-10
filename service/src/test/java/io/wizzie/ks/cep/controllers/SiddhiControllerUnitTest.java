package io.wizzie.ks.cep.controllers;

import io.wizzie.ks.cep.model.*;
import org.junit.Test;
import org.wso2.siddhi.core.stream.output.sink.Sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SiddhiControllerUnitTest {


    @Test
    public void addStreamDefinitionsUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        SourceModel sourceModel = new SourceModel("stream","topic");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("stream","topic");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        AttributeModel attributeModel = new AttributeModel("attributeName", "string");
        List<AttributeModel> attributeModelList = new LinkedList<>();
        attributeModelList.add(attributeModel);
        StreamModel streamModel = new StreamModel("stream",attributeModelList);
        List<StreamModel> streamModelList = new LinkedList<>();
        streamModelList.add(streamModel);

        InOutStreamModel inOutStreamModel = new InOutStreamModel(sourceModelList, sinkModelList, streamModelList);
        siddhiController.addStreamDefinition(inOutStreamModel);
        siddhiController.generateExecutionPlans();
    }


    @Test
    public void addRulesDefinitionUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        String id = "1";
        String version = "v1";
        List<String> streams = Collections.singletonList("placeholder");
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streams, executionPlan);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);

        ProcessingModel processingModel = new ProcessingModel(ruleModelList);
        siddhiController.addRulesDefinition(processingModel);
        siddhiController.generateExecutionPlans();
    }

}
