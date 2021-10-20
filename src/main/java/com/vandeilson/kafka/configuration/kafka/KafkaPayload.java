package com.vandeilson.kafka.configuration.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Builder
@Getter
public class KafkaPayload<T> {

    private final KafkaHeader headers;
    private final T body;

}
