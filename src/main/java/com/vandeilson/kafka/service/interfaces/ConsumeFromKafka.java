package com.vandeilson.kafka.service.interfaces;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface ConsumeFromKafka {

    void send(String topic, KafkaConsumer<String, String> kafkaConsumer);

}
