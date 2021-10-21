package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.kafka.Producers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping
@Slf4j
public class FirstController {

    @GetMapping
    public String getHelloWorld() {
        KafkaProducer<String, String> producer = Producers.getProducer();

        for (int i = 0; i <= 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", Integer.toString(i));
            producer.send(producerRecord);
        }

        // flush and close producer
        producer.close();

        return "Hello, world!";
    }

}
