package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class SiddhiAppBuilderUnitTest {



    @Test
    public void generateStreamDefinition() {

        SiddhiAppBuilder siddhiAppBuilder = new SiddhiAppBuilder();

        String streamDefinition = siddhiAppBuilder.generateStreamDefinition( new StreamModel("stream1", Arrays.asList(
                new AttributeModel("timestamp", "long")
        )));

        assertEquals("@config(async = 'true') define stream stream1 (timestamp long, KAFKA_KEY string);",streamDefinition);
    }

    @Test
    public void generateRuleDefinition() {

        SiddhiAppBuilder siddhiAppBuilder = new SiddhiAppBuilder();
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        String streamDefinition = siddhiAppBuilder.generateRuleDefinition(new RuleModel("1", "v1", streamMapModel, "myExecutionPlan", null));

        assertEquals("@info(name = '1') myExecutionPlan ;",streamDefinition);
    }

    @Test
    public void validateSiddhiPlan() {
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        List<RuleModel> rules = Arrays.asList(
                new RuleModel("1", "v1", streamMapModel, "from streamName select * insert into sinkName", null)
        );

        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamName", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        ProcessingModel processingModel = new ProcessingModel(rules,streamsModel);
        SiddhiAppBuilder siddhiAppBuilder = new SiddhiAppBuilder();

        assertTrue(siddhiAppBuilder.validateSiddhiPlan(processingModel));
    }

    @Test
    public void noValidateSiddhiPlan() {
        SourceModel sourceModel = new SourceModel("streamName", "kafkaTopic", null);
        SinkModel sinkModel = new SinkModel("sinkName", "kafkaTopic", null);
        StreamMapModel streamMapModel = new StreamMapModel(Arrays.asList(sourceModel), Arrays.asList(sinkModel));
        List<RuleModel> rules = Arrays.asList(
                new RuleModel("1", "v1", streamMapModel, "from streamName selecst * insert into sinkName", null)
        );

        List<StreamModel> streamsModel = Arrays.asList(
                new StreamModel("streamName", Arrays.asList(
                        new AttributeModel("timestamp", "long")
                )));

        ProcessingModel processingModel = new ProcessingModel(rules,streamsModel);
        SiddhiAppBuilder siddhiAppBuilder = new SiddhiAppBuilder();

        boolean isValid;
        try {
           isValid = siddhiAppBuilder.validateSiddhiPlan(processingModel);
        }catch(Exception e){
            isValid = false;
        }
        assertFalse(isValid);
    }
}
