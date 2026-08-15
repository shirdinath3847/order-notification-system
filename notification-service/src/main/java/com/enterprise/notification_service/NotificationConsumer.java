package com.enterprise.notification_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    // Configures 3 total attempts with a 2-second delay between retries
    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 2000, multiplier = 1.5),
            autoCreateTopics = "true"
    )
    @KafkaListener(topics = "order-events", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrderEvent(String message) {
        log.info("📩 Processing Order Event: {}", message);

        // Simulation: force a failure if the item is "FAIL" or contains error keywords
        if (message.contains("FAIL")) {
            log.error("⚠️ Simulating processing failure for message: {}", message);
            throw new RuntimeException("Simulated notification service failure!");
        }

        log.info("✅ Notification sent successfully for event: {}", message);
    }

    // Handler for messages that failed all retry attempts
    @DltHandler
    public void handleDlt(
            String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.error("🚨 [DEAD LETTER QUEUE] Message permanently failed!");
        log.error("🚨 Diverted from topic: {} at offset: {}", topic, offset);
        log.error("🚨 Payload: {}", message);
        // In production: trigger an alert, write to an error DB, or notify admin
    }
}