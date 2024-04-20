package com.farhad.example.demo.si.kafka.channeladapter.inbound;

import java.util.concurrent.CountDownLatch;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class CountDownLatchHandler implements MessageHandler {

    private final CountDownLatch countDownLatch = new CountDownLatch(10) ;


    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
    
        log.info("Received: {}000", message);
        // countDownLatch.countDown(); 
        
    }

    

}
