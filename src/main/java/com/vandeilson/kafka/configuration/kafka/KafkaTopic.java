package com.vandeilson.kafka.configuration.kafka;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum KafkaTopic {

    KAFKA(KafkaTopic.TEST_TOPIC_NAME);

    public static final String TEST_TOPIC_NAME = "test_topic";
    public final String topicName;

}
