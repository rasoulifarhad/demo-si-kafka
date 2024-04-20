package com.farhad.example.demo.si.kafka.channeladapter.inboundpushbasedflow;

import static com.farhad.example.demo.si.kafka.channeladapter.inboundpushbasedflow.KafkaConstants.KAFKA_INBOUND_PUSH_FLOW_TOPIC;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.extern.slf4j.Slf4j;


@Configuration
@Slf4j
public class Boot {
    

    @Bean
    @DependsOn("topicListenerFromKafkaFlow")
    public ApplicationRunner run(KafkaTemplate<String,String> kafkaTemplate,
                                @Qualifier("receivedChannel") DirectChannel receivedChannel) {

        receivedChannel.subscribe(message -> log.info("Received {} ",message.getPayload()));
        return args ->  {

            for (int i = 0; i < 10; i++) {

                log.info("Sending: {} " ,"Message # "+ i);
                kafkaTemplate.send(KAFKA_INBOUND_PUSH_FLOW_TOPIC,"Message # "+ i);

            }

        };
    }
}
