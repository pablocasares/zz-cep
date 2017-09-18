package io.wizzie.ks.cep.model;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StreamMapModelModelUnitTest {

    @Test
    public void sourcesIsNotNullTest() {
        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);
        assertNotNull(streamMapModel.sourcesModel);
        assertEquals(sourceModelList, streamMapModel.sourcesModel);
    }

    @Test
    public void sinksNotNullTest() {
        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);
        assertNotNull(streamMapModel.sinksModel);
        assertEquals(sinkModelList, streamMapModel.sinksModel);
    }

    @Test
    public void toStringIsCorrectTest() {
        SourceModel sourceModel = new SourceModel("stream", "input1");
        List<SourceModel> sourceModelList = new LinkedList<>();
        sourceModelList.add(sourceModel);

        SinkModel sinkModel = new SinkModel("streamoutput", "output1");
        List<SinkModel> sinkModelList = new LinkedList<>();
        sinkModelList.add(sinkModel);

        StreamMapModel streamMapModel = new StreamMapModel(sourceModelList, sinkModelList);

        assertNotNull(streamMapModel.sourcesModel);
        assertEquals(sourceModelList, streamMapModel.sourcesModel);

        assertNotNull(streamMapModel.sinksModel);
        assertEquals(sinkModelList, streamMapModel.sinksModel);

        assertEquals(
                "{in: [{streamName: stream, kafkaTopic: input1}], out: [{streamName: streamoutput, kafkaTopic: output1}]}", streamMapModel.toString());
    }

}
