package com.enterprise.order_service;

import com.enterprise.order.grpc.OrderGrpcServiceGrpc;
import com.enterprise.order.grpc.OrderRequest;
import com.enterprise.order.grpc.OrderResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@GrpcService
public class OrderGrpcServiceImpl extends OrderGrpcServiceGrpc.OrderGrpcServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OrderGrpcServiceImpl.class);

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;

    public OrderGrpcServiceImpl(OrderRepository orderRepository, OutboxEventRepository outboxEventRepository) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @Override
    @Transactional
    public void createOrder(OrderRequest request, StreamObserver<OrderResponse> responseObserver) {
        log.info("Received gRPC createOrder request for Order ID: {}", request.getOrderId());

        // 1. Save Order Entity to Database
        OrderEntity order = new OrderEntity();
        order.setOrderId(request.getOrderId());
        order.setItem(request.getItem());
        order.setAmount(request.getAmount());
        order.setCustomerId(request.getCustomerId());
        order.setStatus("CREATED");
        order.setCreatedAt(LocalDateTime.now());
        orderRepository.save(order);

        // 2. Save Outbox Record within the SAME database transaction
        String eventPayload = String.format(
                "{\"orderId\":\"%s\", \"item\":\"%s\", \"customerId\":\"%s\", \"amount\":%.2f, \"status\":\"CREATED\"}",
                order.getOrderId(), order.getItem(), order.getCustomerId(), order.getAmount()
        );

        OutboxEvent outboxEvent = new OutboxEvent("ORDER", order.getOrderId(), "order-events", eventPayload);
        outboxEventRepository.save(outboxEvent);

        log.info("Order and Outbox record saved atomically to PostgreSQL for Order ID: {}", order.getOrderId());

        // 3. Return gRPC response matching OrderResponse
        OrderResponse response = OrderResponse.newBuilder()
                .setOrderId(order.getOrderId())
                .setStatus("SUCCESS")
                .setMessage("Order created and queued via Transactional Outbox")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}