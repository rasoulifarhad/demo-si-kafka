package com.farhad.example.demo.si.kafka.outbound;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import static com.farhad.example.demo.si.kafka.outbound.KafkaConstants.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class OutboundConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String  BootstrapServers;


    @Bean 
    public QueueChannel successChannel() {
        return MessageChannels
                    .queue()
                    .get();
    }

    @Bean("toKafka")
    public MessageChannel toKafka() {
        return MessageChannels
                        .direct()
                        .get();
    }

    @Bean
    public KafkaTemplate<String,String> kTemplate(){
         return new KafkaTemplate<>(pf());
    }


    @Bean 
    public ProducerFactory<String,String> pf() {

        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    @ServiceActivator(inputChannel = "toKafka")
    public MessageHandler kafkaOutboundMessagehandler()  {

        KafkaProducerMessageHandler<String,String> kafkaOutboundMessagehandler = 
                                            new KafkaProducerMessageHandler<>(kTemplate());

        kafkaOutboundMessagehandler.setTopicExpression(new LiteralExpression(OUTBOUND_TOPIC0));
        // handler.setMessageKeyExpression(new LiteralExpression("someKey"));
        kafkaOutboundMessagehandler.setSendSuccessChannelName("successChannel");
        kafkaOutboundMessagehandler.setSendFailureChannelName(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME);
        return kafkaOutboundMessagehandler;
    }
}
