package io.wizzie.cep.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RuleModelUnitTest {

    @Test
    public void idIsNotNullTest() {
        String id = "1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, streamMapModel, executionPlan, null);

        assertNotNull(ruleModelObject.id);
        assertEquals(id, ruleModelObject.getId());
    }

    @Test
    public void streamsIsNotNullTest() {
        String id = "1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, streamMapModel, executionPlan, null);

        assertNotNull(ruleModelObject.streamMapModel);
    }

    @Test
    public void executionPlanIsNotNullTest() {
        String id = "1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, streamMapModel, executionPlan, null);

        assertNotNull(ruleModelObject.executionPlan);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());
    }

    @Test
    public void optionsIsNotNullTest() {
        String id = "1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";
        Map<String, Object> options = new HashMap<>();
        options.put("filterOutputNullDimension", true);

        RuleModel ruleModelObject = new RuleModel(id, streamMapModel, executionPlan, options);

        assertNotNull(ruleModelObject.options);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());
    }

    @Test
    public void toStringIsCorrectTest() {
        String id = "1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, streamMapModel, executionPlan, null);

        assertNotNull(ruleModelObject.id);
        assertEquals(id, ruleModelObject.getId());

        assertNotNull(ruleModelObject.streamMapModel);
        assertEquals(streamMapModel, ruleModelObject.getStreams());

        assertNotNull(ruleModelObject.executionPlan);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());

        assertEquals(
                "{id: 1, streams: {in: [{streamName: streamName, kafkaTopic: kafkaTopic}], out: " +
                        "[{streamName: sinkName, kafkaTopic: kafkaTopic}]}, executionPlan: placeholder, options: null}",
                ruleModelObject.toString());

    }

    @Test
    public void toStringWithOptionsIsCorrectTest() {
        String id = "1";
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String executionPlan = "placeholder";
        Map<String, Object> options = new HashMap<>();
        options.put("filterOutputNullDimension", true);

        RuleModel ruleModelObject = new RuleModel(id, streamMapModel, executionPlan, options);

        assertNotNull(ruleModelObject.id);
        assertEquals(id, ruleModelObject.getId());

        assertNotNull(ruleModelObject.streamMapModel);
        assertEquals(streamMapModel, ruleModelObject.getStreams());

        assertNotNull(ruleModelObject.executionPlan);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());

        assertNotNull(ruleModelObject.options);
        assertEquals(options, ruleModelObject.options);

        assertEquals(
                "{id: 1, streams: {in: [{streamName: streamName, kafkaTopic: kafkaTopic}], out: " +
                        "[{streamName: sinkName, kafkaTopic: kafkaTopic}]}, executionPlan: placeholder, options: {filterOutputNullDimension=true}}",
                ruleModelObject.toString());

    }
}
