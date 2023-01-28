package com.farhad.example.demo.si.kafka.inbound;

import static com.farhad.example.demo.si.kafka.inbound.KafkaConstants.ANOTHER_KAFKA_FLOW_GROUP;
import static com.farhad.example.demo.si.kafka.inbound.KafkaConstants.ANOTHER_KAFKA_FLOW_TOPIC;
import static com.farhad.example.demo.si.kafka.inbound.KafkaConstants.KAFKA_FLOW_GROUP;
import static com.farhad.example.demo.si.kafka.inbound.KafkaConstants.KAFKA_FLOW_TOPIC;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter.ListenerMode;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.support.RetryTemplate;

import lombok.extern.slf4j.Slf4j;
@Configuration
@Slf4j
public class ConsumerChannelConfig {
    
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.spring-integration-kafka}")
    private String springIntegrationKafkaTopic ;



    @Bean
    public DirectChannel consumingChannel() {
        return  MessageChannels
                    .direct()
                    .get();
    }

    // @Bean
    // public DirectChannel errorChannel() {
    //     return  MessageChannels
    //                 .direct()
    //                 .get();
    // }

    @Bean
    public KafkaMessageDrivenChannelAdapter<String,String> kafkaMessageDrivenChannelAdapter(
                                            KafkaMessageListenerContainer<String,String>    container) {

        KafkaMessageDrivenChannelAdapter<String,String> kafkaMessageDrivenChannelAdapter = 
                                            new KafkaMessageDrivenChannelAdapter<>(container);

        kafkaMessageDrivenChannelAdapter.setOutputChannel(consumingChannel());

        return kafkaMessageDrivenChannelAdapter;

    }

    @Bean
    @ServiceActivator(inputChannel = "consumingChannel")
    public CountDownLatchHandler countDownLatchHandler() {
        return new CountDownLatchHandler();
    }

    @Bean
    public KafkaMessageListenerContainer<String,String> kafkaListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties(springIntegrationKafkaTopic);


        return new KafkaMessageListenerContainer<>(consumerFactory(), containerProperties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>  kafkaListenerContainerFactory() {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                                                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String,String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String,Object> consumerConfigs() {
        
        Map<String,Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "integration-group");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return configs;

    } 

        @Bean
    public IntegrationFlow listenerFromKafkaFlow(MessageChannel  errorChannel ) {

        return IntegrationFlows
                    .from(Kafka.messageDrivenChannelAdapter(consumerFactory(),
                                            KafkaMessageDrivenChannelAdapter.ListenerMode.record,
                                           KAFKA_FLOW_TOPIC )
                                .configureListenerContainer(c -> 
                                            c.ackMode(AckMode.MANUAL)
                                            .id("fromKafkaFlowListenerContainer")
                                            .groupId(KAFKA_FLOW_GROUP ))
                                
                                .recoveryCallback(new ErrorMessageSendingRecoverer(errorChannel
                                                                ,new RawRecordHeaderErrorMessageStrategy()))
                                .retryTemplate(new RetryTemplate())
                                .filterInRetry(true))
                    // .filter(Message.class, m ->
                    //              m.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY, String.class) != null,
                    //              f ->  f.throwExceptionOnRejection(true))
                    .<String,String>transform(String::toUpperCase)
                    .channel(c -> c.queue("kafka-flow-result"))
                    .handle( message -> log.info("Received: {}000", message)  )
                    .get();
    }


     
    /**
     * Notice that, in this case, the adapter is given an id ("topic2Adapter"); the container will be registered in the application context 
     * with the name topic2Adapter.container. If the adapter does not have an id property, the container’s bean name will be the container’s 
     * fully qualified class name + #n where n is incremented for each container.
     * 
     */
    @Bean
    public IntegrationFlow anotherListenerFromKafkaFlow(MessageChannel  errorChannel) {

        ConcurrentMessageListenerContainer<String,String> container =  kafkaListenerContainerFactory().createContainer(ANOTHER_KAFKA_FLOW_TOPIC);
        container.getContainerProperties().setGroupId(ANOTHER_KAFKA_FLOW_GROUP);
        return IntegrationFlows
                    .from(Kafka.messageDrivenChannelAdapter(container,
                                                            ListenerMode.record)
                                .id("another-kafka-flow-topic-Adapter")            
                                
                                .recoveryCallback(new ErrorMessageSendingRecoverer(errorChannel
                                                                ,new RawRecordHeaderErrorMessageStrategy()))
                                .retryTemplate(new RetryTemplate())
                                .filterInRetry(true))
                    // .filter(Message.class, m ->
                    //             //  m.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY, Integer.class) < 101,
                    //              m.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY, String.class) != null ,
                    //              f ->  f.throwExceptionOnRejection(true))
                    .<String,String>transform(String::toUpperCase)
                    .channel(c -> c.queue("another-kafka-flow-result"))
                    .handle( message -> log.info("Received: {}000", message)  )
                    .get();
    }

  }         
