package com.vandeilson.kafka.configuration.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaStreamsConfiguration {

    private KafkaStreamsConfiguration() {}

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9091";
    private static final String APPLICATION_ID = "demo-kafka-streams";

    public static Properties getStreamsProperties() {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return properties;
    }
}
