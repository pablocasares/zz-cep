package io.wizzie.ks.cep.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.ks.cep.model.AttributeModel;
import io.wizzie.ks.cep.model.StreamModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.*;

import static io.wizzie.ks.cep.utils.RuleOptionsConstants.FILTER_OUTPUT_NULL_DIMENSION;

public class EventsParser {

    private static final Logger log = LoggerFactory.getLogger(EventsParser.class);

    private static EventsParser instance = null;


    Map<String, StreamModel> eventsFormat = new HashMap<>();

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

    public Object[] parseToObjectArray(String streamName, String key, Map<String, Object> eventData, Map<String, Map<String, String>> inputRenames) {

        //Insert key to eventData
        eventData.put("KAFKA_KEY", key);

        List<Object> attributeList = new LinkedList<>();

        if (eventsFormat.containsKey(streamName)) {
            StreamModel streamModel = eventsFormat.get(streamName);
            attributeList = new ArrayList<>();
            for (AttributeModel attributeModel : streamModel.getAttributes()) {
                Object element;
              
                if(inputRenames.get(streamName) != null && inputRenames.get(streamName).containsKey(attributeModel.getName())){
                    element = eventData.get(inputRenames.get(streamName).get(attributeModel.getName()));
                }else{
                    element = eventData.get(attributeModel.getName());
                }
                attributeList.add(element);
            }
        }//maybe throw exception


        return attributeList.toArray();
    }

    public Map<String, Object> parseToMap(List<Attribute> attributeList, Event event, Map<String, Object> options,
                                          Map<String, String> sinkMapper) {

        Map<String, Object> eventData = new HashMap<>();

        // Get all the attributes values from the list of attributes
        int index = 0;
        for (Object object : event.getData()) {
            if (options != null && (Boolean) options.get(FILTER_OUTPUT_NULL_DIMENSION) && object == null) {
                log.trace("Filtered null value");
            } else {
                String columnName = attributeList.get(index).getName();
                if (sinkMapper != null && sinkMapper.containsKey(columnName)) {
                    eventData.put(sinkMapper.get(columnName), object);
                } else {
                    eventData.put(columnName, object);
                }
            }
            index++;
        }
        return eventData;
    }
}
