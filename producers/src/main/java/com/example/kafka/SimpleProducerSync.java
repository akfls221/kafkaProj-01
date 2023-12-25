package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

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
        try (kafkaProducer) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world2");
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            LOGGER.info("recordMetadata received partition : {}, offset : {}, timeStamp : {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
