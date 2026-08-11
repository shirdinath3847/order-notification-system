package com.enterprise.notification_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    @KafkaListener(topics = "order-events", groupId = "notification-group")
    public void consumeOrderEvent(String message) {

        // Print bright console borders so it's easy to spot
        System.out.println("\n==================================================");
        System.out.println("📩 [NOTIFICATION SERVICE] KAFKA EVENT RECEIVED!");
        System.out.println("📦 Payload Data: " + message);
        System.out.println("📧 Action: Sending email notification to customer...");
        System.out.println("==================================================\n");
    }
}