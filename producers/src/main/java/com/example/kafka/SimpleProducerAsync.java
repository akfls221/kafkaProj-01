package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerAsync {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer Configuration Setting
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer Object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //KafkaProducer Message Send
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world2");

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                LOGGER.info("recordMetadata received partition : {}, offset : {}, timeStamp : {}", metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                LOGGER.error("exception error from broker : {}", exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
