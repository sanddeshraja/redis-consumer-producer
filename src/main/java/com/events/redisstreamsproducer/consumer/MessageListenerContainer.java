package com.events.redisstreamsproducer.consumer;

import com.events.redisstreamsproducer.PurchaseEvent;
import com.events.redisstreamsproducer.PurchaseStreamListener;
import com.events.redisstreamsproducer.consumer.PurchaseEventConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

@Configuration
public class MessageListenerContainer {
    @Value("${stream.key:purchase-stream}")
    private String streamKey;

    @Autowired
    PurchaseEventConsumer purchaseEventConsumer;

    @Bean
    public Subscription subscription(RedisConnectionFactory redisConnectionFactory) throws UnknownHostException {
        purchaseEventConsumer.createConsumerIfNotExists(redisConnectionFactory, streamKey, streamKey);
        StreamOffset<String> streamOffset = StreamOffset.create(streamKey, ReadOffset.lastConsumed());
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, PurchaseEvent>> options
                = StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofMillis(100))
                .targetType(PurchaseEvent.class)
                .build();
        StreamMessageListenerContainer<String, ObjectRecord<String, PurchaseEvent>> container = StreamMessageListenerContainer
                .create(redisConnectionFactory, options);
        Subscription subscription = container.receiveAutoAck(Consumer.from(streamKey, InetAddress.getLocalHost().getHostName()), streamOffset, purchaseStreamListener());
        container.start();
        return subscription;
    }
    @Bean
    public StreamListener<String, ObjectRecord<String, PurchaseEvent>> purchaseStreamListener() {
        return new PurchaseStreamListener();
    }

}
