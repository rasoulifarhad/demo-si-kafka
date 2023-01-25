package com.farhad.example.demo.si.kafka;

import java.util.Collections;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class Boot {
    
    @Value("${kafka.topic.spring-integration-kafka}")
    private String springIntegrationKafkaTopic ;


	// private final CountDownLatchHandler countDownLatchHandler;

	private final MessageChannel producingChannel;

    @Bean
    public ApplicationRunner runMe() {

        return args ->  {

            Map<String,Object> headers = Collections.singletonMap(KafkaHeaders.TOPIC, springIntegrationKafkaTopic);


            log.info("Sending 10 message");
    
            for (int i = 0; i < 10; i++) {
                
                GenericMessage<String> message =  new GenericMessage<>(String.format("Message # %s", i),headers);
                producingChannel.send(message);
    
                log.info("Sended: {}", message);
    
            }
            // countDownLatchHandler.getCountDownLatch().wait();
        };

    }
}
