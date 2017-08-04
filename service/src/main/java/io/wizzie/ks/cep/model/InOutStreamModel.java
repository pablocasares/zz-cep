package io.wizzie.ks.cep.model;

import java.util.List;

public class InOutStreamModel {
    List<SourceModel> sources;
    List<SinkModel> sinks;
    List<StreamModel> streams;

    public List<SourceModel> getSources() {
        return sources;
    }

    public void setSources(List<SourceModel> sources) {
        this.sources = sources;
    }

    public List<SinkModel> getSinks() {
        return sinks;
    }

    public void setSinks(List<SinkModel> sinks) {
        this.sinks = sinks;
    }

    public List<StreamModel> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamModel> streams) {
        this.streams = streams;
    }
}
