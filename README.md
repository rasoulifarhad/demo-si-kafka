# demo-si-kafka
#
## Outbound Channel Adapter
#
# The Outbound channel adapter is used to publish messages from a Spring Integration channel to Kafka topics. 
# 
# The channel is defined in the application context and then wired into the application that sends messages to 
# Kafka. Sender applications can publish to Kafka via Spring Integration messages, which are internally converted 
# to Kafka messages by the outbound channel adapter, as follows: 
# 
#     the payload of the Spring Integration message will be used to populate the payload of the Kafka message, 
# 
#     and (by default) the kafka_messageKey header of the Spring Integration message will be used to populate 
#     the key of the Kafka message.
#
# The target topic and partition for publishing the message can be customized through the kafka_topic and 
# kafka_partitionId headers, respectively.
# 
# NOTE : If the adapter is configured with a topic or message key (either with a constant or expression), those 
# are used and the corresponding header is ignored. If you wish the header to override the configuration, you 
# need to configure it in an expression, such as:
# 
#      topic-expression="headers['topic'] != null ? headers['topic'] : 'myTopic'".
#
# The adapter requires a KafkaTemplate.