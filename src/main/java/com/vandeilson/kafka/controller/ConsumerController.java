package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.kafka.Consumers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.time.Duration;
import java.util.Collections;

@Controller
@Slf4j
@RequestMapping("/consumer")
public class ConsumerController {

    @GetMapping
    public void standardConsumer() {

        KafkaConsumer<String, String> consumer = Consumers.getStandardConsumer();

        consumer.subscribe(Collections.singleton("first_topic"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key:" + record.key() + "\tValue: " + record.value());
                log.info("Partition: " + record.partition() + "\tOffset: " + record.offset());
            }
        }

    }
}
