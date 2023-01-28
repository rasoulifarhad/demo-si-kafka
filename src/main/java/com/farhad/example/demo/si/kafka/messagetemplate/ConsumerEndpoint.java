package com.farhad.example.demo.si.kafka.messagetemplate;

import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ConsumerEndpoint {
    
    
    @ServiceActivator(inputChannel = "upcaseChannel")
    public String upcase(String in) {

        return in.toUpperCase();
    }

    @ServiceActivator(inputChannel = "upcaseMessageChannel")
    public Message<String> upcaseMessage(Message<String> in) {
            
        log.info("Received: {}", in);
        return MessageBuilder
                       .withPayload(in.getPayload().toUpperCase())
                       .copyHeaders(in.getHeaders())
                       .build(); 
    }

}
