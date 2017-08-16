package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RuleModelUnitTest {

    @Test
    public void idIsNotNullTest() {
        String id = "1";
        String version = "v1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        assertNotNull(ruleModelObject.id);
        assertEquals(id, ruleModelObject.getId());
    }

    @Test
    public void versionIsNotNullTest() {
        String id = "1";
        String version = "v1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        assertNotNull(ruleModelObject.version);
        assertEquals(version, ruleModelObject.getVersion());
    }

    @Test
    public void streamsIsNotNullTest() {
        String id = "1";
        String version = "v1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        assertNotNull(ruleModelObject.streamMapModel);
        assertEquals(version, ruleModelObject.getVersion());
    }

    @Test
    public void executionPlanIsNotNullTest() {
        String id = "1";
        String version = "v1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        assertNotNull(ruleModelObject.executionPlan);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());
    }

    @Test
    public void toStringIsCorrectTest() {
        String id = "1";
        String version = "v1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic");
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic");
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streamMapModel, executionPlan);

        assertNotNull(ruleModelObject.id);
        assertEquals(id, ruleModelObject.getId());

        assertNotNull(ruleModelObject.version);
        assertEquals(version, ruleModelObject.getVersion());

        assertNotNull(ruleModelObject.streamMapModel);
        assertEquals(streamMapModel, ruleModelObject.getStreams());

        assertNotNull(ruleModelObject.executionPlan);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());

        assertEquals(
                "{id: 1, version: v1, streams: {in: [{streamName: streamName, kafkaTopic: kafkaTopic}], out: " +
                        "[{streamName: sinkName, kafkaTopic: kafkaTopic}]}, executionPlan: placeholder}",
                ruleModelObject.toString());

    }

}
