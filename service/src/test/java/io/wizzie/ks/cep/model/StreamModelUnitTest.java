package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class StreamModelUnitTest {

    @Test
    public void streamNameIsNotNullTest() {
        String streamName = "myStream";
        List<AttributeModel> attributes = Collections.EMPTY_LIST;

        StreamModel streamModel = new StreamModel(streamName, attributes);

        assertNotNull(streamModel.streamName);
        assertEquals(streamName, streamModel.getStreamName());
    }

    @Test
    public void attributesNotNullTest() {
        String streamName = "myStream";
        List<AttributeModel> attributes =
                Arrays.asList(
                        new AttributeModel("attr1", "float"),
                        new AttributeModel("attr2", "integer"),
                        new AttributeModel("attr3", "boolean")
                );

        StreamModel streamModel = new StreamModel(streamName, attributes);
        assertNotNull(streamModel.attributes);
        assertTrue(streamModel.getAttributes().containsAll(attributes));
        assertTrue(streamModel.getAttributes().get(3).getName().equals("KAFKA_KEY"));
        assertTrue(streamModel.getAttributes().get(3).getAttributeType().equals("string"));
    }

    @Test
    public void toStringIsCorrectTest() {
        String streamName = "myStream";
        List<AttributeModel> attributes =
                Arrays.asList(
                        new AttributeModel("attr1", "float"),
                        new AttributeModel("attr2", "integer"),
                        new AttributeModel("attr3", "boolean")
                );

        StreamModel streamModel = new StreamModel(streamName, attributes);

        assertNotNull(streamModel.streamName);
        assertEquals(streamName, streamModel.getStreamName());

        assertNotNull(streamModel.attributes);
        assertTrue(streamModel.getAttributes().containsAll(attributes));
        assertTrue(streamModel.getAttributes().get(3).getName().equals("KAFKA_KEY"));
        assertTrue(streamModel.getAttributes().get(3).getAttributeType().equals("string"));

        assertEquals(
                "{streamName: myStream, attributes: [{name: attr1, type: float}, {name: attr2, type: integer}, {name: attr3, type: boolean}, {name: KAFKA_KEY, type: string}]}", streamModel.toString());
    }


}
