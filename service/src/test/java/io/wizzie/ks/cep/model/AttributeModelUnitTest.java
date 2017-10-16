package io.wizzie.ks.cep.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AttributeModelUnitTest {

    @Test
    public void nameIsNotNullTest() {
        String attribute = "attribute";
        String attributeType = "long";

        AttributeModel attributeModel = new AttributeModel(attribute, attributeType);

        assertNotNull(attributeModel.name);
        assertEquals(attribute, attributeModel.getName());
    }

    @Test
    public void typeIsNotNullTest() {
        String attribute = "attribute";
        String attributeType = "long";

        AttributeModel attributeModel = new AttributeModel(attribute, attributeType);

        assertNotNull(attributeModel.attributeType);
        assertEquals("long", attributeModel.getAttributeType());
    }

    @Test
    public void toStringIsCorrectTest() {
        String attribute = "attribute";
        String attributeType = "long";

        AttributeModel attributeModel = new AttributeModel(attribute, attributeType);

        assertNotNull(attributeModel.name);
        assertEquals(attribute, attributeModel.getName());

        assertNotNull(attributeModel.attributeType);
        assertEquals(attributeType.toString(), attributeType.toString());

        assertEquals("{name: attribute, type: long}", attributeModel.toString());
    }

}
