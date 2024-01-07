package com.practice.kafka.producer;

import com.practice.kafka.model.OrderModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;

public class OrderSerdeProducer {

    public static final Logger logger = LoggerFactory.getLogger(OrderSerdeProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "order-serde-topic";

        // KafkaProducer Configuration Setting
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());

        //KafkaProducer Object creation
        KafkaProducer<String, OrderModel> kafkaProducer = new KafkaProducer<>(properties);

        String filePath = "C:\\Users\\82106\\Desktop\\taekwan\\kafkaProj-01\\practice\\src\\main\\resources\\pizza_sample.txt";

        //kafkaProducer 객체  생성 -> ProducerRecords 생성 -> send)() 비동기 방식 전송
        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String filePath) {
        final String delimiter = ",";
        String line = "";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                OrderModel orderModel = new OrderModel(tokens[1], tokens[2], tokens[3], tokens[4],
                        tokens[5], tokens[6], LocalDateTime.parse(tokens[7].trim(), dateTimeFormatter));

                sendMessage(kafkaProducer, topicName, key, orderModel);
            }
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String key, OrderModel value) {
        ProducerRecord<String, OrderModel> producerRecord = new ProducerRecord<>(topicName, key, value);
        logger.info("key : {}, value : {}", key, value);

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("recordMetadata received partition : {}, offset : {}", metadata.partition(), metadata.offset());
            } else {
                logger.error("exception error from broker : {}", exception.getMessage());
            }
        });
    }
}
