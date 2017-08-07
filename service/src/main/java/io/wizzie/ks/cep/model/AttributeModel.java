package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AttributeModel {
    String name;
    AttributeType attributeType;

    enum AttributeType {
        STRING("string"),
        INTEGER("int"),
        LONG("long"),
        FLOAT("float"),
        DOUBLE("double"),
        BOOLEAN("bool"),
        OBJECT("object");

        private final String type;

        AttributeType(final String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

    }

    @JsonCreator
    public AttributeModel(@JsonProperty("name") String name,
                          @JsonProperty("type") String attributeType) {
        this.name = name;
        this.attributeType = AttributeType.valueOf(attributeType.toUpperCase());
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public void setName(String name) {
        this.name = name;
    }


    @JsonProperty
    public void setAttributeType(AttributeType attributeType) {
        this.attributeType = attributeType;
    }

    public String getAttributeType() {
        return attributeType.getType();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("name: ").append(name).append(", ")
                .append("type: ").append(attributeType.toString().toLowerCase())
                .append("}");

        return sb.toString();
    }
}
