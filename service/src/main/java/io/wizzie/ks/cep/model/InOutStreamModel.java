package io.wizzie.ks.cep.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class InOutStreamModel {
    List<SourceModel> sources;
    List<SinkModel> sinks;
    List<StreamModel> streams;

    public InOutStreamModel(@JsonProperty("sources") List<SourceModel> sources,
                            @JsonProperty("sinks") List<SinkModel> sinks,
                            @JsonProperty("streams") List<StreamModel> streams) {
        this.sources = sources;
        this.sinks = sinks;
        this.streams = streams;
    }

    @JsonProperty
    public List<SourceModel> getSources() {
        return sources;
    }
    @JsonProperty
    public void setSources(List<SourceModel> sources) {
        this.sources = sources;
    }
    @JsonProperty
    public List<SinkModel> getSinks() {
        return sinks;
    }
    @JsonProperty
    public void setSinks(List<SinkModel> sinks) {
        this.sinks = sinks;
    }
    @JsonProperty
    public List<StreamModel> getStreams() {
        return streams;
    }
    @JsonProperty
    public void setStreams(List<StreamModel> streams) {
        this.streams = streams;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("{")
                .append("sources: ").append(sources).append(", ")
                .append("sinks: ").append(sinks).append(", ")
                .append("streams: ").append(streams)
                .append("}");

        return sb.toString();
    }
}
