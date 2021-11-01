package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.kafka.ConsumersConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.time.Duration;
import java.util.Collections;

@Controller
@Slf4j
@RequestMapping("/consumer")
public class ConsumerController {

//    @GetMapping
//    public void standardConsumer() {
//
//        KafkaConsumer<String, String> consumer = Consumers.getStandardConsumer();
//
//        consumer.subscribe(Collections.singleton("first_topic"));
//
//        while(true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//
//            for (ConsumerRecord<String, String> record : records) {
//                log.info("Key:" + record.key() + "\tValue: " + record.value());
//                log.info("Partition: " + record.partition() + "\tOffset: " + record.offset());
//            }
//        }
//
//    }

    @GetMapping("/groups/as")
    public void consumerGroups() {

        KafkaConsumer<String, String> consumer = ConsumersConfiguration.getAssignAndSeekConsumer();

        // assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
        long offsetToReadFrom = 15L;

        // assign
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        //loop
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while(keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                log.info("Key:" + record.key() + "\tValue: " + record.value());
                log.info("Partition: " + record.partition() + "\tOffset: " + record.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
