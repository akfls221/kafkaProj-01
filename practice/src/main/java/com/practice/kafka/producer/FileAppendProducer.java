package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {
    public static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class.getName());

    public static void main(String[] args) {
        String topicName = "file-topic";

        // KafkaProducer Configuration Setting
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer Object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        File file = new File("C:\\Users\\82106\\Desktop\\taekwan\\kafkaProj-01\\practice\\src\\main\\resources\\pizza_append.txt");

        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, false);
        FileEventSource fileEventSource = new FileEventSource(1000, file, eventHandler);
        Thread fileEventSourceThread = new Thread(fileEventSource);

        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }
}
