package com.enterprise.order_service;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, String> {
    // JpaRepository gives us save(), findById(), findAll(), deleteById() automatically!
}