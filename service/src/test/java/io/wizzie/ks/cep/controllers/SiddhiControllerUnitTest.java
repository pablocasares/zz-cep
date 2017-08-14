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
    public void addProcessingDefinitionsUnitTest() {
        SiddhiController siddhiController = SiddhiController.getInstance();

        //Add Sources and Sinks Definition

        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "input1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);


        //////////////////////////////////

        //Add Rule Definition

        String id = "rule1";
        String version = "v1";
        String executionPlan = "from stream select * insert into streamoutput";

        StreamMap streamMap = new StreamMap(Arrays.asList(sourceModel), Arrays.asList(sinkModel));

        RuleModel ruleModelObject = new RuleModel(id, version, streamMap, executionPlan);

        List<RuleModel> ruleModelList = new LinkedList<>();
        ruleModelList.add(ruleModelObject);


        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream1", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        //////////////////////////////////


        ProcessingModel processingModel = new ProcessingModel(ruleModelList, streamsModel);

        siddhiController.addProcessingDefinition(processingModel);
    }

}
