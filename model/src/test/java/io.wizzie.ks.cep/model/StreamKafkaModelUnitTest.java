package io.wizzie.ks.cep.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StreamKafkaModelUnitTest {

    @Test
    public void streamNameNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        StreamKafkaModel streamKafkaModel = new StreamKafkaModel(streamName, kafkaTopic);

        assertNotNull(streamKafkaModel.streamName);
        assertEquals(streamName, streamKafkaModel.getStreamName());
    }

    @Test
    public void kafkaTopicNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        StreamKafkaModel streamKafkaModel = new StreamKafkaModel(streamName, kafkaTopic);

        assertNotNull(streamKafkaModel.kafkaTopic);
        assertEquals(kafkaTopic, streamKafkaModel.getKafkaTopic());
    }

    @Test
    public void toStringIsCorrectTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        StreamKafkaModel streamKafkaModel = new StreamKafkaModel(streamName, kafkaTopic);

        assertNotNull(streamKafkaModel.streamName);
        assertEquals(streamName, streamKafkaModel.getStreamName());

        assertNotNull(streamKafkaModel.kafkaTopic);
        assertEquals(kafkaTopic, streamKafkaModel.getKafkaTopic());

        assertEquals("{streamName: stream, kafkaTopic: topic}", streamKafkaModel.toString());
    }
}
