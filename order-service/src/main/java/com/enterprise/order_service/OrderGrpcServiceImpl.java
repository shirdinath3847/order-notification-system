package com.enterprise.order_service;

import com.enterprise.order.grpc.OrderGrpcServiceGrpc;
import com.enterprise.order.grpc.OrderRequest;
import com.enterprise.order.grpc.OrderResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class OrderGrpcServiceImpl extends OrderGrpcServiceGrpc.OrderGrpcServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OrderGrpcServiceImpl.class);

    @Override
    public void createOrder(OrderRequest request, StreamObserver<OrderResponse> responseObserver) {
        log.info("Received gRPC request for Order ID: {} and Item: {}", request.getOrderId(), request.getItem());

        // Build the response payload
        OrderResponse response = OrderResponse.newBuilder()
                .setOrderId(request.getOrderId())
                .setStatus("SUCCESS")
                .setMessage("Order processed successfully via gRPC!")
                .build();

        // Send response back to the client
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}