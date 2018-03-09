package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SinkModelUnitTest {

    @Test
    public void streamNameNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SinkModel sinkModel = new SinkModel(streamName, kafkaTopic, null);

        assertNotNull(sinkModel.streamName);
        assertEquals(streamName, sinkModel.getStreamName());
    }

    @Test
    public void kafkaTopicNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SinkModel sinkModel = new SinkModel(streamName, kafkaTopic, null);

        assertNotNull(sinkModel.kafkaTopic);
        assertEquals(kafkaTopic, sinkModel.getKafkaTopic());
    }

    @Test
    public void dimMapperNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";
        Map<String, String> dimMapper = new HashMap<>();

        SinkModel sinkModel = new SinkModel(streamName, kafkaTopic, dimMapper);

        assertNotNull(sinkModel.dimMapper);
        assertEquals(dimMapper, sinkModel.getDimMapper());
    }

    @Test
    public void toStringIsCorrectTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SinkModel sinkModel = new SinkModel(streamName, kafkaTopic, null);

        assertNotNull(sinkModel.streamName);
        assertEquals(streamName, sinkModel.getStreamName());

        assertNotNull(sinkModel.kafkaTopic);
        assertEquals(kafkaTopic, sinkModel.getKafkaTopic());

        assertEquals("{streamName: stream, kafkaTopic: topic}", sinkModel.toString());
    }

}
