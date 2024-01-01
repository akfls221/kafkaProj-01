package com.paractice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "file-topic";

        // KafkaProducer Configuration Setting
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer Object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String filePath = "C:\\Users\\82106\\Desktop\\taekwan\\kafkaProj-01\\practice\\src\\main\\resources\\pizza_sample.txt";

        //kafkaProducer 객체  생성 -> ProducerRecords 생성 -> send)() 비동기 방식 전송
        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        final String delimiter = ",";
        String line = "";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for (int i = 1; i < tokens.length; i++) {
                    if (i != tokens.length) {
                        value.append(tokens[i] + delimiter);
                    } else {
                        value.append(tokens[i]);
                    }
                }

                sendMessage(kafkaProducer, topicName, key, value.toString());
            }
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
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
