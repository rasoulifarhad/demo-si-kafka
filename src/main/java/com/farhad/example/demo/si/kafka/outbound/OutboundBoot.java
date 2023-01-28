package com.farhad.example.demo.si.kafka.outbound;

import static com.farhad.example.demo.si.kafka.outbound.KafkaConstants.*;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class OutboundBoot {


    @Bean
    @Order(600)
    public ApplicationRunner runOutboundProducer(@Qualifier("toKafka") MessageChannel toKafka ) {
        return args -> {

            log.info("Sending 10 message to {}", OUTBOUND_TOPIC0);
    
            String payload ;
            for (int i = 0; i < 10; i++) {
                payload = String.format("Message # %s", i);
                boolean  sended = toKafka.send(new GenericMessage<>(payload));

                log.info("{} send result:  {}", payload,sended);
            }
        };
    
    }
}
