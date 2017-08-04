package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
                        new AttributeModel("attr1", AttributeModel.AttributeType.FLOAT),
                        new AttributeModel("attr2", AttributeModel.AttributeType.INTEGER),
                        new AttributeModel("attr3", AttributeModel.AttributeType.BOOLEAN)
                );

        StreamModel streamModel = new StreamModel(streamName, attributes);

        assertNotNull(streamModel.attributes);
        assertEquals(attributes, streamModel.getAttributes());
    }

    @Test
    public void toStringIsCorrectTest() {
        String streamName = "myStream";
        List<AttributeModel> attributes =
                Arrays.asList(
                        new AttributeModel("attr1", AttributeModel.AttributeType.FLOAT),
                        new AttributeModel("attr2", AttributeModel.AttributeType.INTEGER),
                        new AttributeModel("attr3", AttributeModel.AttributeType.BOOLEAN)
                );

        StreamModel streamModel = new StreamModel(streamName, attributes);

        assertNotNull(streamModel.streamName);
        assertEquals(streamName, streamModel.getStreamName());

        assertNotNull(streamModel.attributes);
        assertEquals(attributes, streamModel.getAttributes());

        assertEquals(
                "{streamName: myStream, attributes: [{name: attr1, type: float}, {name: attr2, type: int}, {name: attr3, type: bool}]}", streamModel.toString());

    }


}
