package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InOutStreamModelUnitTest {

    @Test
    public void sourcesIsNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        List<SourceModel> sources = Collections.singletonList(new SourceModel(streamName, kafkaTopic));
        List<SinkModel> sinks = Collections.EMPTY_LIST;
        List<StreamModel> streams = Collections.EMPTY_LIST;

        InOutStreamModel inOutStreamModel = new InOutStreamModel(sources, sinks, streams);

        assertNotNull(inOutStreamModel.sources);
        assertEquals(sources, inOutStreamModel.getSources());
    }

    @Test
    public void sinksIsNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        List<SourceModel> sources = Collections.EMPTY_LIST;
        List<SinkModel> sinks = Collections.singletonList(new SinkModel(streamName, kafkaTopic));
        List<StreamModel> streams = Collections.EMPTY_LIST;

        InOutStreamModel inOutStreamModel = new InOutStreamModel(sources, sinks, streams);

        assertNotNull(inOutStreamModel.sinks);
        assertEquals(sinks, inOutStreamModel.getSinks());
    }

    @Test
    public void streamsIsNotNullTest() {
        String streamName = "myStream";
        List<AttributeModel> attributes =
                Arrays.asList(
                        new AttributeModel("attr1", "float"),
                        new AttributeModel("attr2", "integer"),
                        new AttributeModel("attr3", "boolean")
                );

        List<SourceModel> sources = Collections.EMPTY_LIST;
        List<SinkModel> sinks = Collections.EMPTY_LIST;
        List<StreamModel> streams = Collections.singletonList(new StreamModel(streamName, attributes));

        InOutStreamModel inOutStreamModel = new InOutStreamModel(sources, sinks, streams);

        assertNotNull(inOutStreamModel.streams);
        assertEquals(streams, inOutStreamModel.getStreams());
    }

    @Test
    public void toStringIsCorrectTest() {
        String sourceStreamName = "sourceStream";
        String sourceKafkaTopic = "sourceTopic";

        String sinkStreamName = "sinkStream";
        String sinkKafkaTopic = "sinkTopic";

        String streamName = "myStream";
        List<AttributeModel> attributes =
                Arrays.asList(
                        new AttributeModel("attr1", "float"),
                        new AttributeModel("attr2", "integer"),
                        new AttributeModel("attr3", "boolean")
                );

        List<SourceModel> sources = Collections.singletonList(new SourceModel(sourceStreamName, sourceKafkaTopic));
        List<SinkModel> sinks = Collections.singletonList(new SinkModel(sinkStreamName, sinkKafkaTopic));
        List<StreamModel> streams = Collections.singletonList(new StreamModel(streamName, attributes));

        InOutStreamModel inOutStreamModel = new InOutStreamModel(sources, sinks, streams);

        assertNotNull(inOutStreamModel.sources);
        assertEquals(sources, inOutStreamModel.getSources());

        assertNotNull(inOutStreamModel.sinks);
        assertEquals(sinks, inOutStreamModel.getSinks());

        assertNotNull(inOutStreamModel.streams);
        assertEquals(streams, inOutStreamModel.getStreams());

        assertEquals(
                "{" +
                        "sources: [{streamName: sourceStream, kafkaTopic: sourceTopic}], " +
                        "sinks: [{streamName: sinkStream, kafkaTopic: sinkTopic}], " +
                        "streams: [{streamName: myStream, attributes: [{name: attr1, type: float}, {name: attr2, type: integer}, {name: attr3, type: boolean}]}]" +
                        "}", inOutStreamModel.toString());

    }

}
