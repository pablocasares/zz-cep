package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ProcessingModelUnitTest {

    @Test
    public void rulesIsNotNullTest() {
        List<RuleModel> rules = Arrays.asList(
                new RuleModel("1", "v1", Collections.EMPTY_LIST, "myExecutionPlan"),
                new RuleModel("2", "v1", Collections.EMPTY_LIST, "myOtherPlan")
        );

        ProcessingModel processingModel = new ProcessingModel(rules);

        assertNotNull(processingModel.rules);
        assertEquals(rules, processingModel.getRules());
        assertEquals(2, processingModel.getRules().size());
    }

    @Test
    public void toStringIsCorrectTest() {
        List<RuleModel> rules = Arrays.asList(
                new RuleModel("1", "v1", Collections.singletonList("stream1"), "myExecutionPlan"),
                new RuleModel("2", "v1", Arrays.asList("stream1", "stream2"), "myOtherPlan")
        );

        ProcessingModel processingModel = new ProcessingModel(rules);

        assertNotNull(processingModel.rules);
        assertEquals(rules, processingModel.getRules());
        assertEquals(2, processingModel.getRules().size());

        assertEquals("{rules: [" +
                "{id: 1, version: v1, streams: [stream1], executionPlan: myExecutionPlan}, " +
                "{id: 2, version: v1, streams: [stream1, stream2], executionPlan: myOtherPlan}" +
                "]}", processingModel.toString());
    }


}
