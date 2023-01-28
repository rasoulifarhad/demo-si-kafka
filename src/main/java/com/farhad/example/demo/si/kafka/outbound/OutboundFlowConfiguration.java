package com.farhad.example.demo.si.kafka.outbound;

import static com.farhad.example.demo.si.kafka.outbound.KafkaConstants.OUTBOUND_TOPIC1;
import static com.farhad.example.demo.si.kafka.outbound.KafkaConstants.OUTBOUND_TOPIC2;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.dsl.KafkaProducerMessageHandlerSpec;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;


@Configuration
public class OutboundFlowConfiguration {
    

    @Value("${kafka.bootstrap-servers}")
    private String  BootstrapServers;

    @Bean("flowProducerFactory") 
    public ProducerFactory<String,String> pf() {

        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public DefaultKafkaHeaderMapper mapper() {
        return new DefaultKafkaHeaderMapper() ;
    }

    private KafkaProducerMessageHandlerSpec<String,String,?>  kafkaMessageHandler(ProducerFactory<String,String> pf , String topic) {

        return Kafka
                .outboundChannelAdapter(pf)
                .messageKey(m -> m
                                .getHeaders() 
                                .get(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER))
                .headerMapper(mapper())
                .partitionId(m -> 10)
                .topicExpression("headers[kafka_topic] ?: '" + topic + "'"  )
                .configureKafkaTemplate(t -> t.id("kafkaTemplate:" + topic));

    }
    @Bean
    public IntegrationFlow sendToKafkaFlow(@Qualifier("flowProducerFactory") ProducerFactory<String,String> pf) {

        return flow -> { 
                  flow
                    .publishSubscribeChannel(c -> c
                                    .subscribe(f -> f.handle(kafkaMessageHandler(pf,OUTBOUND_TOPIC1)
                                                      .timestampExpression("T(Long).valueOf('1487694048633')"),
                                                      e -> e.id("kafkaOutboundProducer1") ) )
                                    .subscribe(f -> f.handle(kafkaMessageHandler(pf,OUTBOUND_TOPIC2)
                                                    .timestamp(m -> 1487694048644L),
                                                      e -> e.id("kafkaOutboundProducer2") ) )
                                           );  

        };

    }
}
