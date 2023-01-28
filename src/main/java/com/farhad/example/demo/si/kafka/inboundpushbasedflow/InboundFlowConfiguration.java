package com.farhad.example.demo.si.kafka.inboundpushbasedflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter.ListenerMode;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.support.RetryTemplate;

import lombok.extern.slf4j.Slf4j;

import static com.farhad.example.demo.si.kafka.inboundpushbasedflow.KafkaConstants.* ;

@Configuration
@Slf4j
public class InboundFlowConfiguration {
    
    @Value("${kafka.bootstrap-servers}")
    private String  BootstrapServers;

    @Bean("receivedChannel")
    public DirectChannel receivedChannel() {

        return MessageChannels
                    .direct()
                    .get();
    }

    @Bean
    public ConsumerFactory<String,String> consumerFactory() {

        Map<String,Object>  configs = new HashMap<>();

        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        

        return new DefaultKafkaConsumerFactory<>(configs);
        
    } 

    @Bean
    public IntegrationFlow topicListenerFromKafkaFlow(MessageChannel errorChannel,
                                                       @Qualifier("receivedChannel") DirectChannel receivedChannel) {

        return IntegrationFlows
                        .from(Kafka.messageDrivenChannelAdapter(consumerFactory(),
                                                ListenerMode.record,
                                                KAFKA_INBOUND_PUSH_FLOW_TOPIC)
                                    
                                    .configureListenerContainer(c -> 
                                                        c.ackMode(AckMode.MANUAL)
                                                          .groupId(KAFKA_INBOUND_PUSH_FLOW_GROUP)
                                                         .id(KAFKA_INBOUND_PUSH_FLOW_TOPIC + "ListenerContainer"))
                                    .recoveryCallback(new ErrorMessageSendingRecoverer(errorChannel,
                                                                                    new RawRecordHeaderErrorMessageStrategy()))
                                    .retryTemplate(new RetryTemplate())
                                    .filterInRetry(true))
                        // .filter(null, null)
                        .<String,String>transform(String::toUpperCase)
                        .channel(c -> c.queue("receivedChannelQueue"))
                        // .channel(receivedChannel)
                        .handle(message -> log.info("Handle, Received: {}",message.getPayload()))
                        .get();
    }

}
