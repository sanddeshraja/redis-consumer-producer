package com.events.redisstreamsproducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;


@Slf4j
@Service
public class PurchaseStreamListener implements StreamListener<String, ObjectRecord<String, PurchaseEvent>> {

    Logger log = LoggerFactory.getLogger(PurchaseStreamListener.class);
    @Value("${stream.key:purchase-events}")
    private String streamKey;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, PurchaseEvent> record) {

        PurchaseEvent purchaseEvent = record.getValue();

        try {
            redisTemplate.opsForValue().set(purchaseEvent.getPurchaseId(),
                    objectMapper.writeValueAsString(purchaseEvent));
            log.error("Consumed");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        redisTemplate.opsForStream().acknowledge(streamKey, record);
    }
}