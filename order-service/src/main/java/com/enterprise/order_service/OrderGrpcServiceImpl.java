package com.enterprise.order_service;

import com.enterprise.order.grpc.OrderGrpcServiceGrpc;
import com.enterprise.order.grpc.OrderRequest;
import com.enterprise.order.grpc.OrderResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

@GrpcService
public class OrderGrpcServiceImpl extends OrderGrpcServiceGrpc.OrderGrpcServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OrderGrpcServiceImpl.class);

    private final OrderProducer orderProducer;
    private final OrderRepository orderRepository; // Inject Repository

    public OrderGrpcServiceImpl(OrderProducer orderProducer, OrderRepository orderRepository) {
        this.orderProducer = orderProducer;
        this.orderRepository = orderRepository;
    }

    @Override
    public void createOrder(OrderRequest request, StreamObserver<OrderResponse> responseObserver) {
        log.info("Received gRPC request for Order ID: {} and Item: {}", request.getOrderId(), request.getItem());

        // 1. Save initial record in PostgreSQL with PENDING status
        OrderEntity entity = new OrderEntity(
                request.getOrderId(),
                request.getCustomerId(),
                request.getAmount(),
                request.getItem(),
                "PENDING",
                LocalDateTime.now()
        );
        orderRepository.save(entity);
        log.info("Order saved to PostgreSQL database with status PENDING");

        // 2. Publish Event to Kafka Docker Broker
        orderProducer.sendOrderEvent(request.getOrderId(), request.getItem());

        // 3. Update status to COMPLETED after event is dispatched
        entity.setStatus("COMPLETED");
        orderRepository.save(entity);

        // 4. Return gRPC Response back to Client
        OrderResponse response = OrderResponse.newBuilder()
                .setOrderId(request.getOrderId())
                .setStatus("SUCCESS")
                .setMessage("Order saved to DB and Kafka event published!")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}