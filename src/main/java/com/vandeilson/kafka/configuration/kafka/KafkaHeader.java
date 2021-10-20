package com.vandeilson.kafka.configuration.kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class KafkaHeader {

    private final String contentType;
    private final String eventName;

}
