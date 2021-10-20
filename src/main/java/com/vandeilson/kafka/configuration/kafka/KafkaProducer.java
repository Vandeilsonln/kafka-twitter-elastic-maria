package com.vandeilson.kafka.configuration.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.internals.Topic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(final Topic topic, final String key, final KafkaPayload<?> kafkaPayload) {
        //
    }


}
