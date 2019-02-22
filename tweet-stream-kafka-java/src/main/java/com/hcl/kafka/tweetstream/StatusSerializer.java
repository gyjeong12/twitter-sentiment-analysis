package com.hcl.kafka.tweetstream;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import twitter4j.Status;

import java.util.Map;

public class StatusSerializer implements Serializer<Status> {


    @Override public void configure(Map<String, ?> configs, boolean b) {
    }


    @Override
    public byte[] serialize(String arg0, Status arg1) {
        byte[] retVal = null;

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(arg1).getBytes();
            System.out.println("Successful serialization!");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return retVal;
    }


    @Override public void close() {
    }
}
