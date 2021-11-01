package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.kafka.ProducersConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;


@RestController
@RequestMapping("/producer")
@Slf4j
public class ProducerController {

    @GetMapping
    public String getHelloWorld() {
        KafkaProducer<String, String> producer = ProducersConfiguration.getProducer();

        for (int i = 0; i <= 1; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", Integer.toString(i));
            producer.send(producerRecord, (m, e) -> {   // new Callback( RecordMetadata rm, Exception e) -> É uma interface funcional
                if (e == null) {
                    log.info("Received new matadata \n" +
                        "Topic:\t" + m.topic() +
                        "\nPartition:\t" + m.partition() +
                        "\nOffset:\t" + m.offset() +
                        "\nTimestamp:\t" + m.timestamp());
                } else {
                    log.error("Error while producing.\n" +
                        e);
                }
            });

        }

        // flush and close producer
        producer.close();

        return "Hello, world!";
    }

    @GetMapping("/keys")
    public String producerWithKeys() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = ProducersConfiguration.getProducer();

        for (int i = 0; i <= 10; i++) {
            String value = Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            log.info("Key:\t" + key);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("new_topic", key, value);

            producer.send(producerRecord, (m, e) -> {   // new Callback( RecordMetadata rm, Exception e) -> É uma interface funcional
                if (e == null) {
                    log.info("Received new matadata \n" +
                        "Topic:\t" + m.topic() +
                        "\nPartition:\t" + m.partition() +
                        "\nOffset:\t" + m.offset() +
                        "\nTimestamp:\t" + m.timestamp() + "\n");
                } else {
                    log.error("Error while producing.\n" +
                        e);
                }}).get(); // NEVER DO THIS IN PRODUCTION
        }

        return "Producer with keys";
    }

}
