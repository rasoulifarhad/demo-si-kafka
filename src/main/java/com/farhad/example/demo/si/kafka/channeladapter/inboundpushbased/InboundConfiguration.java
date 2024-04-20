package com.farhad.example.demo.si.kafka.channeladapter.inboundpushbased;

import static com.farhad.example.demo.si.kafka.channeladapter.inboundpushbased.KafkaConstants.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter.ListenerMode;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.messaging.MessageChannel;

@Configuration
public class InboundConfiguration {
    
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

        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public KafkaMessageListenerContainer<String,String> container() {

        ContainerProperties containerProperties = new ContainerProperties(KAFKA_INBOUND_PUSH_TOPIC);
        containerProperties.setGroupId(KAFKA_INBOUND_PUSH_GROUP);

        return new KafkaMessageListenerContainer<>(consumerFactory(), containerProperties);

    }

    @Bean
    public KafkaMessageDrivenChannelAdapter<String,String> adapter(KafkaMessageListenerContainer<String, String> container,
                                                        @Qualifier("receivedChannel") DirectChannel receivedChannel) {

        KafkaMessageDrivenChannelAdapter<String,String> kafkaMessageDrivenChannelAdapter = 
                                                new KafkaMessageDrivenChannelAdapter<>(container,ListenerMode.record);

        kafkaMessageDrivenChannelAdapter.setOutputChannel(receivedChannel);
        return kafkaMessageDrivenChannelAdapter;

    }
}
