package com.example.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerPartitionAssign {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPartitionAssign.class.getName());

    public static void main(String[] args) {
        String topicName = "pizza-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        kafkaConsumer.assign(List.of(topicPartition));

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("main program starts to exit by calling wakeup");
//            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        pollmanualCommit(kafkaConsumer);

    }

    private static void pollmanualCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                LOGGER.info("############### loopCnt : {} consumerRecords count : {}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    LOGGER.info("record key : {}, record value : {}, partitions : {}, recordOffset : {}", consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset());
                }

                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync(); //동기방식의 offset commit - 커밋 실패시 retry를 하다 결국 필요없어 졌을때 Exception을 던진다.
                        LOGGER.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    LOGGER.error(e.getMessage());
                }

            }
        } catch (WakeupException e) {
            LOGGER.error("wakeup exception has been called");
        } finally {
            LOGGER.error("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}
