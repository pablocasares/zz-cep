package io.wizzie.ks.cep.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SourceModelUnitTest {

    @Test
    public void streamNameNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SourceModel sourceModel = new SourceModel(streamName, kafkaTopic);

        assertNotNull(sourceModel.streamName);
        assertEquals(streamName, sourceModel.getStreamName());
    }

    @Test
    public void kafkaTopicNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SourceModel sourceModel = new SourceModel(streamName, kafkaTopic);

        assertNotNull(sourceModel.kafkaTopic);
        assertEquals(kafkaTopic, sourceModel.getKafkaTopic());
    }

    @Test
    public void toStringIsCorrectTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        SourceModel sourceModel = new SourceModel(streamName, kafkaTopic);

        assertNotNull(sourceModel.streamName);
        assertEquals(streamName, sourceModel.getStreamName());

        assertNotNull(sourceModel.kafkaTopic);
        assertEquals(kafkaTopic, sourceModel.getKafkaTopic());

        assertEquals("{streamName: stream, kafkaTopic: topic}", sourceModel.toString());
    }

}
