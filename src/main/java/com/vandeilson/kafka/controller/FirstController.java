package com.vandeilson.kafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
@RequestMapping
@Slf4j
public class FirstController {

    @GetMapping
    public String getHelloWorld() {
        // create Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9091";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record

        // send data - asynchronous
        for (int i = 0; i <= 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", Integer.toString(i));

            producer.send(record);

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

        return "Hello, world!";
    }

}
