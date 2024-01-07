package com.practice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.practice.kafka.model.OrderModel;
import com.practice.kafka.producer.OrderSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<OrderModel> {

    public static final Logger logger = LoggerFactory.getLogger(OrderDeserializer.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public OrderModel deserialize(String topic, byte[] data) {
        OrderModel orderModel = null;

        try {
            orderModel = this.objectMapper.readValue(data, OrderModel.class);
        } catch (IOException e) {
            logger.error("Json Deserialization exception :" + e.getMessage());
        }

        return orderModel;
    }
}
