package com.events.redisstreamsproducer.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.stereotype.Component;

@Component
public class PurchaseEventConsumer {
    @Value("${stream.key:purchase-stream}")
    private String streamKey;
    Logger log = LoggerFactory.getLogger(PurchaseEventConsumer.class);
    public void createConsumerIfNotExists(RedisConnectionFactory redisConnectionFactory, String streamKey, String groupName) {
        try{
            redisConnectionFactory.getConnection().streamCommands().xGroupCreate(streamKey.getBytes(), groupName, ReadOffset.from("0-0"), true);
        } catch (RedisSystemException ex){
            log.error("Failed to create consumer group: {}", ex.getMessage());
        }
    }
}
