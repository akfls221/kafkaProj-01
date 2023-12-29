package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducerCustomPartitioner {
    public static final Logger LOGGER = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class.getName());

    public static void sendPizzaMessage (KafkaProducer<String, String> kafkaProducer, String topicName,
                                         int iterCount, int interIntervalMillis,
                                         int intervalMillis, int intervalCount, boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq++ != iterCount) {
            HashMap<String, String> pizzaOrderMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pizzaOrderMessage.get("key"), pizzaOrderMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pizzaOrderMessage, sync);

            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    LOGGER.info("########## IntervalCount : " + intervalCount + " intervalMillis : ", intervalMillis + "#########");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    LOGGER.info("interIntervalMillis : " + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pizzaOrderMessage, boolean sync) {

        if (!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    LOGGER.info("async message : {}, partition : {}, offset : {}", pizzaOrderMessage.get("key"), metadata.partition(), metadata.offset());
                } else {
                    LOGGER.error("exception error from broker : {}", exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                LOGGER.info("sync message : {}, partition : {}, offset : {}", pizzaOrderMessage.get("key"), metadata.partition(), metadata.offset());

            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        String topicName = "pizza-topic-partitioner";

        // KafkaProducer Configuration Setting
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        properties.setProperty("custom.specialKey", "P001");

        //KafkaProducer Object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        sendPizzaMessage(kafkaProducer, topicName,
                -1, 10,
                100, 100, true);

        kafkaProducer.close();
    }
}
