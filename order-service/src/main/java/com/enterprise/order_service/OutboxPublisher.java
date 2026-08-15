package com.enterprise.order_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // Runs every 2 seconds
    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findPendingEvents();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Found {} pending outbox event(s) to publish", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                // Synchronously send to Kafka
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload()).get();

                // Mark processed
                event.setStatus("PROCESSED");
                event.setProcessedAt(LocalDateTime.now());
                outboxEventRepository.save(event);

                log.info("Successfully published Outbox event ID: {} for Order ID: {}", event.getId(), event.getAggregateId());
            } catch (Exception e) {
                log.error("Failed to publish Outbox event ID: {}. Error: {}", event.getId(), e.getMessage());
                // Will retry on next scheduled execution
            }
        }
    }
}