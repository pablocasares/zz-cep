package io.wizzie.ks.cep.builder;

import org.wso2.siddhi.core.SiddhiManager;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SiddhiManager {

    List<String> streamDefinitions = new LinkedList<String>();
    List<String> queryDefinitions = new LinkedList<String>();
    Map<String, String> executionPlans = new HashMap<String,String>();
    org.wso2.siddhi.core.SiddhiManager siddhiManager;

    public SiddhiManager() {
        siddhiManager = new org.wso2.siddhi.core.SiddhiManager();
    }

    public void addStreamDefinition (String streamDefinition){
        this.streamDefinitions.add(streamDefinition);
    }

    public void addQueryDefinition (String queryDefinition){
        this.queryDefinitions.add(queryDefinition);
    }

    public SiddhiManager(String definition, String query){

    }

}
