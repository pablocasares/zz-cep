package io.wizzie.cep.model;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StreamKafkaModelUnitTest {

    @Test
    public void streamNameNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        StreamKafkaModel streamKafkaModel = new StreamKafkaModel(streamName, kafkaTopic, null);

        assertNotNull(streamKafkaModel.streamName);
        assertEquals(streamName, streamKafkaModel.getStreamName());
    }

    @Test
    public void kafkaTopicNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        StreamKafkaModel streamKafkaModel = new StreamKafkaModel(streamName, kafkaTopic, null);

        assertNotNull(streamKafkaModel.kafkaTopic);
        assertEquals(kafkaTopic, streamKafkaModel.getKafkaTopic());
    }

    @Test
    public void dimMapperNotNullTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";
        Map<String, String> dimMapper = new HashMap<>();

        StreamKafkaModel streamKafkaModel = new StreamKafkaModel(streamName, kafkaTopic, dimMapper);

        assertNotNull(streamKafkaModel.dimMapper);
        assertEquals(dimMapper, streamKafkaModel.getDimMapper());
    }

    @Test
    public void toStringIsCorrectTest() {
        String streamName = "stream";
        String kafkaTopic = "topic";

        StreamKafkaModel streamKafkaModel = new StreamKafkaModel(streamName, kafkaTopic, null);

        assertNotNull(streamKafkaModel.streamName);
        assertEquals(streamName, streamKafkaModel.getStreamName());

        assertNotNull(streamKafkaModel.kafkaTopic);
        assertEquals(kafkaTopic, streamKafkaModel.getKafkaTopic());

        assertEquals("{streamName: stream, kafkaTopic: topic}", streamKafkaModel.toString());
    }
}
