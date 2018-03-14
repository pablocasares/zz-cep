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


        @Override
        public String toString() {
            return type;
        }

    }

    @JsonCreator
    public AttributeModel(@JsonProperty("name") String name,
                          @JsonProperty("type") AttributeType attributeType) {
        this.name = name;
        this.attributeType = attributeType;
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
    public AttributeType getType() {
        return attributeType;
    }

    @JsonProperty
    public void setAttributeType(AttributeType attributeType) {
        this.attributeType = attributeType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("name: ").append(name).append(", ")
                .append("type: ").append(attributeType.toString())
                .append("}");

        return sb.toString();
    }
}
