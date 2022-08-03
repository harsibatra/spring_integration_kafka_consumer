package com.example.kafkaconsumer.config;

import com.example.kafkaconsumer.model.Book;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ConsumerChannelConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.topic}")
    private String springIntegrationKafkaTopic;

    @Bean
    public PollableChannel consumerChannel() {
        return new QueueChannel();
    }

    @Bean
    public KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter() {
        KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter(
                kafkaListenerContainer());
        kafkaMessageDrivenChannelAdapter.setOutputChannel(consumerChannel());

        return kafkaMessageDrivenChannelAdapter;
    }

    @SuppressWarnings("unchecked")
    @Bean
    public ConcurrentMessageListenerContainer kafkaListenerContainer() {
        ContainerProperties containerProps = new ContainerProperties(springIntegrationKafkaTopic);

        return (ConcurrentMessageListenerContainer) new ConcurrentMessageListenerContainer<>(
                consumerFactory(), containerProps);
    }

    @Bean
    public ConsumerFactory consumerFactory() {
        return new DefaultKafkaConsumerFactory(consumerConfigs(),new StringDeserializer(),
                new JsonDeserializer<>(Book.class));
    }

    @Bean
    public Map consumerConfigs() {
        Map properties = new HashMap();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dummy");
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return properties;
    }

    @ServiceActivator(inputChannel = "consumerChannel")
    public void getConsumedMessages(Message<Book> msg) {
        Book book = msg.getPayload();
        String key = (String) msg.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
        System.out.println("consumed key is " + key + " and value is "+ book);
    }
}
