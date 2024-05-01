package com.events.redisstreamsproducer.producer;

import com.events.redisstreamsproducer.PurchaseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Objects;


@Service
public class PurchaseStreamProducer {
    private final RedisTemplate<String, String> redisTemplate;
    @Value("${stream.key:purchase-events}")
    private String streamKey;
    Logger logger = LoggerFactory.getLogger(PurchaseStreamProducer.class);


    public PurchaseStreamProducer(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    public RecordId produce(PurchaseEvent purchaseEvent) {
        ObjectRecord<String, PurchaseEvent> purchaseEventRecord = StreamRecords.newRecord()
                .ofObject(purchaseEvent)
                .withStreamKey(streamKey);
        RecordId recordId = this.redisTemplate.opsForStream().add(purchaseEventRecord);
        if(Objects.isNull(recordId)) {
            logger.error("Failed to produce record: {}", purchaseEvent);
            return null;
        }return recordId;
    }
}
