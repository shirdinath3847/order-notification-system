package com.enterprise.order_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(String orderId, String item) {
        String message = String.format("{\"orderId\":\"%s\", \"item\":\"%s\", \"status\":\"CREATED\"}", orderId, item);
        log.info("Publishing Kafka event to topic '{}': {}", TOPIC, message);
        kafkaTemplate.send(TOPIC, orderId, message);
    }
}