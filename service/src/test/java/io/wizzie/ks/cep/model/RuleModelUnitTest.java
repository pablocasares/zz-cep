package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RuleModelUnitTest {

    @Test
    public void idIsNotNullTest() {
        String id = "1";
        String version = "v1";
        List<String> streams = Collections.singletonList("placeholder");
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streams, executionPlan);

        assertNotNull(ruleModelObject.id);
        assertEquals(id, ruleModelObject.getId());
    }

    @Test
    public void versionIsNotNullTest() {
        String id = "1";
        String version = "v1";
        List<String> streams = Collections.singletonList("placeholder");
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streams, executionPlan);

        assertNotNull(ruleModelObject.version);
        assertEquals(version, ruleModelObject.getVersion());
    }

    @Test
    public void streamsIsNotNullTest() {
        String id = "1";
        String version = "v1";
        List<String> streams = Collections.singletonList("placeholder");
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streams, executionPlan);

        assertNotNull(ruleModelObject.streams);
        assertEquals(version, ruleModelObject.getVersion());
    }

    @Test
    public void executionPlanIsNotNullTest() {
        String id = "1";
        String version = "v1";
        List<String> streams = Collections.singletonList("placeholder");
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streams, executionPlan);

        assertNotNull(ruleModelObject.executionPlan);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());
    }

    @Test
    public void toStringIsCorrectTest() {
        String id = "1";
        String version = "v1";
        List<String> streams = Collections.singletonList("placeholder");
        String executionPlan = "placeholder";

        RuleModel ruleModelObject = new RuleModel(id, version, streams, executionPlan);

        assertNotNull(ruleModelObject.id);
        assertEquals(id, ruleModelObject.getId());

        assertNotNull(ruleModelObject.version);
        assertEquals(version, ruleModelObject.getVersion());

        assertNotNull(ruleModelObject.streams);
        assertEquals(streams, ruleModelObject.getStreams());

        assertNotNull(ruleModelObject.executionPlan);
        assertEquals(executionPlan, ruleModelObject.getExecutionPlan());

        assertEquals(
                "{id: 1, version: v1, streams: [placeholder], executionPlan: placeholder}", ruleModelObject.toString());

    }

}
