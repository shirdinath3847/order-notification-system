

package com.enterprise.order_service;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEntity {

    @Id
    private String orderId;

    private String customerId;
    private Double amount;
    private String item;
    private String status; // e.g., PENDING, COMPLETED, FAILED
    private LocalDateTime createdAt;
}