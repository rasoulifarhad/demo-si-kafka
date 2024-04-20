package com.farhad.example.demo.si.kafka.gateway.outbound;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter;

@Configuration
public class OutboundConfiguration {
    
    @Value("${kafka.bootstrap-servers}")
    private String  BootstrapServers;

    /**
     * Notice that the same class as the outbound channel adapter is used, the only difference being that the kafka template passed 
     * into the constructor is a ReplyingKafkaTemplate
     * 
     * The outbound topic, partition, key etc, are determined the same way as the outbound adapter. 
     * 
     * The reply topic is determined as follows:
     * 
     *   1. A message header KafkaHeaders.REPLY_TOPIC, if present (must have a String or byte[] value) - validated against 
     *      the template’s reply container subscribed topics. 
     *   
     *   2. If the template’s replyContainer is subscribed to just one topic, it will be used. 
     * 
     * You can also specify a KafkaHeaders.REPLY_PARTITION header to determine a specific partition to be used for replies. 
     */
    @Bean
    @ServiceActivator(inputChannel = "kafkaRequests" ,outputChannel = "kafkaReplies")
    public KafkaProducerMessageHandler<String,String> outGateway(
                    ReplyingKafkaTemplate<String, String, String> kafkaTemplate) {


        return new KafkaProducerMessageHandler<>(kafkaTemplate);
    }

    @Bean
    public IntegrationFlow outboundGatewayFlow(
                    ReplyingKafkaTemplate<String, String, String> kafkaTemplate) {

        return IntegrationFlows
                        .from("kafkaRequests")
                        .handle(Kafka.outboundGateway(kafkaTemplate))
                        .channel("kafkaReplies")
                        .get();

    }

    @Bean
    ReplyingKafkaTemplate<String, String, String> template(ProducerFactory<String, String> pf,
                                            ConcurrentKafkaListenerContainerFactory<String, String> factory) {
            
        ConcurrentMessageListenerContainer<String, String> replyContainer = factory.createContainer("kafkaReplies");
                            
        replyContainer.getContainerProperties().setGroupId("request.kafkaReplies");

        ReplyingKafkaTemplate<String, String, String> template = new ReplyingKafkaTemplate<>(pf, replyContainer);
        template.setMessageConverter(new ByteArrayJsonMessageConverter());
        template.setDefaultTopic("requests");
        return template;
    }




    // @Bean
    // public IntegrationFlow outboundGatewayFlow2(ConcurrentMessageListenerContainer<String, String> repliesContainer) {

    //     return IntegrationFlows
    //                     .from("kafkaRequests")
    //                     .handle(Kafka.outboundGateway(pf(),repliesContainer)
    //                                  .configureKafkaTemplate(t -> t.defaultReplyTimeout(Duration.ofSeconds(30))))
    //                     .channel("kafkaReplies")
    //                     .get();
                        
                        
    // }

    // @Bean 
    // public ProducerFactory<String,String> pf() {

    //     Map<String,Object> props = new HashMap<>();
    //     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    //     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
    //     props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
    //     return new DefaultKafkaProducerFactory<>(props);
    // }

    // @Bean
    // public ReplyingKafkaTemplate<String, String, String> replyingTemplate(
	// 		                    ProducerFactory<String, String> pf,
	// 	    	                ConcurrentMessageListenerContainer<String, String> repliesContainer) {

	//     return new ReplyingKafkaTemplate<>(pf, repliesContainer);
    // }

    // @Bean
    // public ConcurrentMessageListenerContainer<String, String> repliesContainer(
	// 		ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
			
	//     ConcurrentMessageListenerContainer<String, String> repliesContainer = containerFactory.createContainer("kafkaReplies");
	//     repliesContainer.getContainerProperties().setGroupId("request.kafkaReplies");
	//     // repliesContainer.setAutoStartup(false);
	//     return repliesContainer;
    // }
}
