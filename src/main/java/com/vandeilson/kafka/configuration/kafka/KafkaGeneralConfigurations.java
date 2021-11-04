package com.vandeilson.kafka.configuration.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Properties;

@Component
public class KafkaGeneralConfigurations {

    @Value(value = "${kafka.bootstrap-server}")
    private String bootstrapServer;

    @Value(value = "${kafka.streams.application-id}")
    private String applicationId;

    @Value(value = "${kafka.producer.enable-idempotence}")
    private String enableIdempotence;

    @Value(value = "${kafka.producer.acks}")
    private String acks;

    @Value(value = "${kafka.producer.max.in-flight.requests-per-connection}")
    private String maxRequestsPerConnection;

    @Value(value = "${kafka.producer.compression-type}")
    private String compressionType;

    @Value(value = "${kafka.producer.linger.ms}")
    private String linger;

    @Value(value = "${kafka.consumer.group-id}")
    private String groupId;

    @Value(value = "${kafka.consumer.auto-offset.reset}")
    private String autoOffsetReset;

    @Value(value = "${kafka.consumer.enable.auto-commit}")
    private String enableAutoCommit;

    @Value(value = "${kafka.consumer.max.poll.records}")
    private String maxPollRecords;

    public KafkaProducer<String, String> getProducer() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer (idempotent)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxRequestsPerConnection);

        // high throughout producer (at the expense of a bit of latency and cpu usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, linger);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        return new KafkaProducer<>(properties);
    }

    public KafkaConsumer<String, String> getStandardConsumer(String topic) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public Properties getStreamsProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return properties;
    }
}