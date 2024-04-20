package com.farhad.example.demo.si.kafka.channeladapter.inboundpushbased;

import static com.farhad.example.demo.si.kafka.channeladapter.inboundpushbased.KafkaConstants.*;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.extern.slf4j.Slf4j;


@Configuration
@Slf4j
public class Boot {

    @Bean
    public ApplicationRunner run(KafkaTemplate<String,String> kafkaTemplate,
                                        DirectChannel receivedChannel) {

        receivedChannel.subscribe(message -> log.info("Received {} ",message.getPayload()));
        return args ->  {

            for (int i = 0; i < 10; i++) {

                log.info("Sending: {} " ,"Message # "+ i);
                kafkaTemplate.send(KAFKA_INBOUND_PUSH_TOPIC,"Message # "+ i);
                
                
            }

        };
        
    }
    
}
