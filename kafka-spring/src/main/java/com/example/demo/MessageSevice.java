package com.example.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MessageSevice {
    @KafkaListener(topics = "test5",groupId = "groupe-ms")
    public void message(ConsumerRecord<String,String> cr) throws JsonProcessingException {
        PageEvent pageEvent=pageEvent(cr.value());
        System.out.println("**************");
        System.out.println("key =>"+cr.key());
        System.out.println(pageEvent.getPage()+","+pageEvent.getDate()+","+pageEvent.getDuration());
        System.out.println("***************");
    }
    private PageEvent pageEvent(String JsonPageEvent) throws JsonProcessingException {
        JsonMapper jsonMapper=new JsonMapper();
        PageEvent pageEvent=jsonMapper.readValue(JsonPageEvent,PageEvent.class);
        return pageEvent;
    }
}
