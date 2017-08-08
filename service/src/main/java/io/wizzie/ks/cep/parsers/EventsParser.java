package io.wizzie.ks.cep.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.ks.cep.model.AttributeModel;
import io.wizzie.ks.cep.model.StreamModel;
import org.wso2.siddhi.core.event.Event;

import java.io.IOException;
import java.util.*;

public class EventsParser {

    private static EventsParser instance = null;


    Map<String, StreamModel> eventsFormat = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();

    //Prevent instantiation
    private EventsParser() {
    }

    public static EventsParser getInstance() {
        if (instance == null) {
            instance = new EventsParser();
        }
        return instance;
    }


    public void addEventFormat(String modelName, StreamModel streamModel) {
        eventsFormat.put(modelName, streamModel);
    }

    public void clear() {
        eventsFormat.clear();
    }

    public Object[] parseToObjectArray(String streamName, String event) {

        //get streamName related with topic


        Map<String, Object> eventData = null;
        List<Object> attributeList = new LinkedList<>();
        try {
            eventData = objectMapper.readValue(event, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (eventsFormat.containsKey(streamName)) {
            StreamModel streamModel = eventsFormat.get(streamName);
            attributeList = new ArrayList<>();
            for (AttributeModel attributeModel : streamModel.getAttributes()) {
                Object element = eventData.get(attributeModel.getName());
                attributeList.add(element);
            }
        }//maybe throw exception

        return attributeList.toArray();
    }

    public String parseToString(String streamName, Event event) {

        Map<String, Object> eventData = null;

        if (eventsFormat.containsKey(streamName)) {
            StreamModel streamModel = eventsFormat.get(streamName);
            int i = 0;
            for (AttributeModel attributeModel : streamModel.getAttributes()) {
                Object element = event.getData()[i];
                eventData.put(attributeModel.getName(), element);
                i++;
            }
        }
        String eventString = null;
        try {
            eventString = objectMapper.writeValueAsString(eventData);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return eventString;
    }

}
