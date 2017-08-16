package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ProcessingModelUnitTest {

    @Test
    public void rulesIsNotNullTest() {
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        List<RuleModel> rules = Arrays.asList(
                new RuleModel("1", "v1", streamMapModel, "myExecutionPlan"),
                new RuleModel("2", "v1", streamMapModel, "myOtherPlan")
        );

        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream1", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        ProcessingModel processingModel = new ProcessingModel(rules,streamsModel);

        assertNotNull(processingModel.rules);
        assertEquals(rules, processingModel.getRules());
        assertEquals(2, processingModel.getRules().size());
    }


    @Test
    public void streamsIsNotNullTest() {
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        List<RuleModel> rules = Arrays.asList(
                new RuleModel("1", "v1", streamMapModel, "myExecutionPlan"),
                new RuleModel("2", "v1", streamMapModel, "myOtherPlan")
        );

        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream1", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        ProcessingModel processingModel = new ProcessingModel(rules,streamsModel);

        assertNotNull(processingModel.streams);
        assertEquals(streamsModel, processingModel.getStreams());
        assertEquals(1, processingModel.getStreams().size());
    }


    @Test
    public void toStringIsCorrectTest() {
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        List<RuleModel> rules = Arrays.asList(
                new RuleModel("1", "v1", streamMapModel, "myExecutionPlan"),
                new RuleModel("2", "v1", streamMapModel, "myOtherPlan")
        );

        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("stream1", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        ProcessingModel processingModel = new ProcessingModel(rules,streamsModel);

        assertNotNull(processingModel.rules);
        assertEquals(rules, processingModel.getRules());
        assertEquals(2, processingModel.getRules().size());

        assertNotNull(processingModel.streams);
        assertEquals(streamsModel, processingModel.getStreams());
        assertEquals(1, processingModel.getStreams().size());

        assertEquals("{streams: [{streamName: stream1, attributes: [{name: timestamp, type: long}]}], rules: [{id: 1, " +
                "version: v1, streams: {in: [{streamName: streamName, kafkaTopic: kafkaTopic}], out: [{streamName: " +
                "sinkName, kafkaTopic: kafkaTopic}]}, executionPlan: myExecutionPlan}, {id: 2, version: v1, streams: " +
                "{in: [{streamName: streamName, kafkaTopic: kafkaTopic}], out: [{streamName: sinkName, kafkaTopic: " +
                "kafkaTopic}]}, executionPlan: myOtherPlan}]}\n", processingModel.toString());
    }


}
