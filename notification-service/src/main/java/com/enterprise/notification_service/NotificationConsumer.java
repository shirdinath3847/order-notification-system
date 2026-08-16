package com.enterprise.notification_service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public NotificationConsumer(ProcessedEventRepository processedEventRepository, ObjectMapper objectMapper) {
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 2000, multiplier = 1.5),
            autoCreateTopics = "true"
    )
    @KafkaListener(topics = "order-events", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consumeOrderEvent(String message) {
        log.info("📩 [NOTIFICATION SERVICE] Received Kafka Event: {}", message);

        String orderId = extractOrderId(message);
        if (orderId == null || orderId.isBlank()) {
            log.warn("⚠️ Event has missing or empty orderId. Skipping: {}", message);
            return;
        }

        // 1. Idempotency Check: Skip duplicate events
        if (processedEventRepository.existsByEventId(orderId)) {
            log.warn("🔁 [DUPLICATE DETECTED] Order ID '{}' already processed. Skipping duplicate notification.", orderId);
            return;
        }

        // 2. Simulated failure for retry testing
        if (message.contains("FAIL")) {
            log.error("⚠️ Simulating processing failure for message: {}", message);
            throw new RuntimeException("Simulated notification service failure!");
        }

        // 3. Dispatch Notification logic
        dispatchNotification(orderId, message);

        // 4. Mark as processed in database
        try {
            processedEventRepository.save(new ProcessedEvent(orderId));
            log.info("💾 Marked event '{}' as PROCESSED in database.", orderId);
        } catch (DataIntegrityViolationException e) {
            log.warn("🔁 [CONCURRENT DUPLICATE] Race condition caught by DB unique constraint for order ID '{}'", orderId);
        }
    }

    @DltHandler
    public void handleDlt(
            String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.error("🚨 [DEAD LETTER QUEUE] Message permanently failed after retries!");
        log.error("🚨 Diverted from topic: {} at offset: {}", topic, offset);
        log.error("🚨 Payload: {}", message);
    }

    private String extractOrderId(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            return jsonNode.has("orderId") ? jsonNode.get("orderId").asText() : null;
        } catch (Exception e) {
            log.error("❌ Failed to parse JSON message: {}", message, e);
            return null;
        }
    }

    private void dispatchNotification(String orderId, String message) {
        log.info("==================================================");
        log.info("🔔 [NOTIFICATION SENT] Customer alerted for Order: {}", orderId);
        log.info("📦 Payload: {}", message);
        log.info("==================================================");
    }
}