package com.farhad.example.demo.si.kafka.channeladapter.messagetemplate;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class Boot {
    
    @Bean
    @Order(200)
    public ApplicationRunner  runMessageTemplate(@Qualifier("upcaseChannel") MessageChannel upcaseChannel ) {

        return args -> {

            log.info("Sending 10 message to channel {}", "upcaseChannel");

            MessagingTemplate messagingTemplate = new MessagingTemplate();

            String payload ;
            for (int i = 0; i < 10; i++) {
                payload = String.format("Message # %s", i);

                Message<?> reply = messagingTemplate.sendAndReceive(upcaseChannel,new GenericMessage<>(payload));

                log.info("{} send result:  {}", payload,reply.getPayload());
            }


        };
    }

    @Bean
    @Order(200)
    public ApplicationRunner  runMessageMessageTemplate(@Qualifier("upcaseMessageChannel") MessageChannel upcaseMessageChannel ) {

        return args -> {

            log.info("Sending 10 message to channel {}", "upcaseMessageChannel");

            MessagingTemplate messagingTemplate = new MessagingTemplate();

            String payload ;
            for (int i = 0; i < 10; i++) {
                payload = String.format("Message # %s", i);

                Message<?> reply = messagingTemplate.sendAndReceive(upcaseMessageChannel,new GenericMessage<>(payload));

                log.info("{} send result:  {}", payload,reply.getPayload());
            }


        };
    }
}
