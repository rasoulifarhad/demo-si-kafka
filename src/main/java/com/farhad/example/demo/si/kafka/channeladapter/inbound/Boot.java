package com.farhad.example.demo.si.kafka.channeladapter.inbound;

import static com.farhad.example.demo.si.kafka.channeladapter.inbound.KafkaConstants.*;

import java.util.Collections;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
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
    @Order(100)
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

    @Bean
    @Order(200)
    public ApplicationRunner runTestListenerFromKafkaFlow() {
        
        return args -> {
            Map<String,Object> headers = Collections.singletonMap(KafkaHeaders.TOPIC, KAFKA_FLOW_TOPIC);


            log.info("Sending 10 message to {}",KAFKA_FLOW_TOPIC);
    
            for (int i = 0; i < 10; i++) {
                
                GenericMessage<String> message =  new GenericMessage<>(String.format("%s Message # %s",KAFKA_FLOW_TOPIC, i),headers);
                producingChannel.send(message);
    
                log.info("Sended: {}", message);
    
            }
            // countDownLatchHandler.getCountDownLatch().wait();

        };
    }

    @Bean
    @Order(300)
    public ApplicationRunner runTestAnotherListenerFromKafkaFlow() {
        
        return args -> {
            Map<String,Object> headers = Collections.singletonMap(KafkaHeaders.TOPIC, ANOTHER_KAFKA_FLOW_TOPIC);


            log.info("Sending 10 message to {}",ANOTHER_KAFKA_FLOW_TOPIC);
    
            for (int i = 0; i < 10; i++) {
                
                GenericMessage<String> message =  new GenericMessage<>(String.format("%s Message # %s",ANOTHER_KAFKA_FLOW_TOPIC, i),headers);
                producingChannel.send(message);
    
                log.info("Sended: {}", message);
    
            }
            // countDownLatchHandler.getCountDownLatch().wait();

        };
    }

}
