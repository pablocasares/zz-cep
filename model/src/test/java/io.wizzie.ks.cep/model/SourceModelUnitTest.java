package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SourceModelUnitTest {

    @Test
    public void streamNameNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SourceModel sourceModel = new SourceModel(streamName, kafkaTopic, null);

        assertNotNull(sourceModel.streamName);
        assertEquals(streamName, sourceModel.getStreamName());
    }

    @Test
    public void kafkaTopicNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SourceModel sourceModel = new SourceModel(streamName, kafkaTopic, null);

        assertNotNull(sourceModel.kafkaTopic);
        assertEquals(kafkaTopic, sourceModel.getKafkaTopic());
    }

    @Test
    public void dimMapperNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";
        Map<String, String> dimMapper = new HashMap<>();

        SourceModel sourceModel = new SourceModel(streamName, kafkaTopic, dimMapper);

        assertNotNull(sourceModel.dimMapper);
        assertEquals(dimMapper, sourceModel.getDimMapper());
    }

    @Test
    public void toStringIsCorrectTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SourceModel sourceModel = new SourceModel(streamName, kafkaTopic, null);

        assertNotNull(sourceModel.streamName);
        assertEquals(streamName, sourceModel.getStreamName());

        assertNotNull(sourceModel.kafkaTopic);
        assertEquals(kafkaTopic, sourceModel.getKafkaTopic());

        assertEquals("{streamName: stream, kafkaTopic: topic}", sourceModel.toString());
    }

}
