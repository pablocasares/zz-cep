package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


public class StreamMapModel {
    List<SourceModel> sourcesModel;
    List<SinkModel> sinksModel;

    public StreamMapModel(@JsonProperty("in") List<SourceModel> sourcesModel, @JsonProperty("out") List<SinkModel> sinksModel){
        this.sourcesModel = sourcesModel;
        this.sinksModel = sinksModel;
    }

    public List<SourceModel> getSourceModel() {
        return sourcesModel;
    }

    public void setSourceModel(List<SourceModel> sourcesModel) {
        this.sourcesModel = sourcesModel;
    }

    public List<SinkModel> getSinkModel() {
        return sinksModel;
    }

    public void setSinkModel(List<SinkModel> sinksModel) {
        this.sinksModel = sinksModel;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("in: ").append(sourcesModel).append(", ")
                .append("out: ").append(sinksModel)
                .append("}");

        return sb.toString();
    }
}
