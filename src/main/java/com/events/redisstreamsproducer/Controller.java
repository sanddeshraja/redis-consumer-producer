package com.events.redisstreamsproducer;

import com.events.redisstreamsproducer.producer.PurchaseStreamProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class Controller {

    @Autowired
    private PurchaseStreamProducer purchaseStreamProducer;

    @PostMapping("/message")
    public void publishMessage(@RequestBody PurchaseEvent message) {
        purchaseStreamProducer.produce(message);
    }
}
