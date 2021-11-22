package com.vandeilson.kafka.service;

public interface ConsumeFromKafka {

    void sendToDataBase(String topic);

}
