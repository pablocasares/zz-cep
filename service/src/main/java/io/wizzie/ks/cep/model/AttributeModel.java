package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AttributeModel {
    String name;
    AttributeType attributeType;

    enum AttributeType {
        STRING("string"), INTEGER("integer"), OBJECT("object"), DOUBLE("double");

        private final String type;

        private AttributeType(final String type) {
            this.type = type;
        }


        @Override
        public String toString() {
            return type;
        }

    }

    @JsonCreator
    public AttributeModel(@JsonProperty("name") String name,
                          @JsonProperty("type") AttributeType attributeType) {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public AttributeType getAttributeType() {
        return attributeType;
    }

    public void setAttributeType(AttributeType attributeType) {
        this.attributeType = attributeType;
    }
}
