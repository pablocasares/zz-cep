package io.wizzie.ks.cep.controllers;

import io.wizzie.ks.cep.connectors.kafka.Kafka2Siddhi;
import io.wizzie.ks.cep.connectors.kafka.Siddhi2Kafka;
import io.wizzie.ks.cep.model.ProcessingModel;
import io.wizzie.ks.cep.model.RuleModel;
import io.wizzie.ks.cep.model.SinkModel;
import io.wizzie.ks.cep.model.SourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);


    private Map<String, String> kafka2Siddhi = new HashMap<>();
    private Map<String, String> siddhi2Kafka = new HashMap<>();
    private Map<String, Map<String, String>> inputMapper = new HashMap<>();
    final List<Kafka2Siddhi> consumers = new ArrayList<>();
    Siddhi2Kafka producer;

    public KafkaController() {
    }

    public void init(Properties consumerProperties, Properties producerProperties) {
        int numConsumers = 1;
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        for (int i = 0; i < numConsumers; i++) {
            Kafka2Siddhi consumer = new Kafka2Siddhi(consumerProperties);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        producer = new Siddhi2Kafka(producerProperties);
    }

    public void addProcessingModel(ProcessingModel processingModel, Map<String, Map<String, InputHandler>> inputHandlers) {

        //clear existing sources
        kafka2Siddhi.clear();
        for (RuleModel rule : processingModel.getRules()) {
            for (SourceModel sourceModel : rule.getStreams().getSourceModel()) {
                kafka2Siddhi.put(sourceModel.getKafkaTopic(), sourceModel.getStreamName());
                inputMapper.put(sourceModel.getStreamName(), sourceModel.getDimMapper());
            }
        }
        //subscribe to the topics.
        for (Kafka2Siddhi consumer : consumers) {
            //Subscribe to the topics associated with the streams.
            consumer.subscribe(kafka2Siddhi, inputHandlers, inputMapper);
        }

        //clear existing sinks
        siddhi2Kafka.clear();

        for (RuleModel rule : processingModel.getRules()) {
            for (SinkModel sinkModel : rule.getStreams().getSinkModel()) {
                siddhi2Kafka.put(sinkModel.getStreamName(), sinkModel.getKafkaTopic());
            }
        }
        if (producer == null) {
            log.warn("KafkaController has not been initialized. Please exec init method before calling addProcessingModel.");
        }
    }


    public void send2Kafka(String kafkaTopic, String streamName, Event event, Map<String,
            StreamDefinition> streamDefinitionMap, Map<String, Object> options, Map<String, String> sinkMapper) {
        producer.send(kafkaTopic, streamName, event, streamDefinitionMap, options, sinkMapper);
    }

    public void shutdown(){
        for(Kafka2Siddhi kafka2Siddhi : consumers){
            kafka2Siddhi.shutdown();
        }
        producer.close();
    }

}
