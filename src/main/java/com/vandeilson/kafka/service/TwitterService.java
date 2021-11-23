package com.vandeilson.kafka.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.core.Client;
import com.vandeilson.kafka.configuration.client.TwitterClientConfiguration;
import com.vandeilson.kafka.configuration.kafka.KafkaGeneralConfigurations;
import com.vandeilson.kafka.configuration.kafka.Topics;
import com.vandeilson.kafka.service.interfaces.ConsumeFromKafka;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TwitterService {

    @Autowired TwitterClientConfiguration twitter;
    @Autowired KafkaGeneralConfigurations kafkaGeneralConfigurations;

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);
    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public void getRelatedTweets(String keyword, String topic) {

        KafkaProducer<String, String> kafkaProducer = kafkaGeneralConfigurations.getProducer();

        Client twitterClient = twitter.getTwitterClient(msgQueue, keyword);
        twitterClient.connect();

        while (!twitterClient.isDone()) {
            String msg;
            try {
                msg = msgQueue.poll(10, TimeUnit.SECONDS);
                log.info(msg);
                if (msg != null) kafkaProducer.send(new ProducerRecord<>(topic, null, msg));
            } catch (Exception e) {
                log.error(e.getMessage());
                twitterClient.stop();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            log.info("Shutting dows twitter client...");
            twitterClient.stop();
            log.info("Closing producer...");
            kafkaProducer.close();
            log.info("Done!");
        }));
    }

    public void send(String topic, ConsumeFromKafka consumeFromKafka) {
        KafkaConsumer<String, String> kafkaConsumer = kafkaGeneralConfigurations.getStandardConsumer(topic);
        consumeFromKafka.send(topic, kafkaConsumer);

    }

    public void startKafkaStream(String topic) {
        Properties properties = kafkaGeneralConfigurations.getStreamsProperties();

        // create a topology
        StreamsBuilder builder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = builder.stream(topic);
        KStream<String, String> filteredStream = inputTopic.filter((k, v) -> extractUserFollowers(v) > 1000);
        filteredStream.to(Topics.ELASTIC_IMPORTANT.getTopicName());

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        // start our streams applications
        kafkaStreams.start();
    }

    private int extractUserFollowers(final String tweetJson) {
        try {
            return objectMapper.readTree(tweetJson)
                .get("user").get("follower_count").asInt();
        } catch (Exception e) {
            return 0;
        }
    }

}
