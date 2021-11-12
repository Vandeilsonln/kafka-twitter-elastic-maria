package com.vandeilson.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.twitter.hbc.core.Client;
import com.vandeilson.kafka.configuration.client.ElasticSearchClientConfiguration;
import com.vandeilson.kafka.configuration.client.TwitterClientConfiguration;
import com.vandeilson.kafka.configuration.kafka.KafkaGeneralConfigurations;
import com.vandeilson.kafka.model.dtos.TweetDataDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TwitterService {

    @Autowired
    TwitterClientConfiguration twitter;

    @Autowired
    ElasticSearchClientConfiguration elasticSearch;

    @Autowired
    KafkaGeneralConfigurations kafkaGeneralConfigurations;

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);
    private final JsonParser jsonParser = new JsonParser();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void getRelatedTweets(String keyword) {

        KafkaProducer<String, String> kafkaProducer = kafkaGeneralConfigurations.getProducer();

        Client twitterClient = twitter.getTwitterClient(msgQueue, keyword);
        twitterClient.connect();

        while (!twitterClient.isDone()) {
            String msg;
            try {
                msg = msgQueue.poll(10, TimeUnit.SECONDS);
                log.info(msg);
                if (msg != null) kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg));
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

    public void sendToElasticSearch(String topic) {

        RestHighLevelClient esClient = elasticSearch.getClient();
        KafkaConsumer<String, String> kafkaConsumer = kafkaGeneralConfigurations.getStandardConsumer(topic);

        while(true) {
            try {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                log.info("Received: " + records.count() + " records");
                BulkRequest bulkRequest = fillBulkBatch(records);

                if (records.count() > 0) {
                    esClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    log.info("Commiting Offseets...");
                    kafkaConsumer.commitSync();
                    log.info("Offsets have been committed.");
                }
            } catch (Exception e) {
                log.error("There was a problem: ");
                log.error(e.getMessage());
            }
        }
    }

    public void startKafkaStream() {
        Properties properties = kafkaGeneralConfigurations.getStreamsProperties();

        // create a topology
        StreamsBuilder builder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = builder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter((k, v) -> extractUserFollowers(v) > 1000);
        filteredStream.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        // start our streams applications
        kafkaStreams.start();
    }

    private BulkRequest fillBulkBatch(final ConsumerRecords<String, String> records) {
        BulkRequest bulkRequest = new BulkRequest();

        for (ConsumerRecord<String, String> i : records) {

            // --- Kafka Generic ID ---
            // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

            // --- Twitter Feed Id ---
            try {
                String jsonRecord = i.value();
                String twitterId = extractTwitterId(jsonRecord);

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", twitterId)
                    .source(jsonRecord, XContentType.JSON);

                bulkRequest.add(indexRequest);
            } catch (NullPointerException e) {
                log.warn("Skipped bad data: " + i.value());
            }
        }
        return bulkRequest;
    }

    private String extractTwitterId(final String tweetJson) {
        // gson library
        return jsonParser.parse(tweetJson)
            .getAsJsonObject()
            .get("id_str")
        .getAsString();
    }

    private int extractUserFollowers(final String tweetJson) {
        // gson library
        try {
            return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("user")
                .getAsJsonObject()
                .get("followers_count")
                .getAsInt();
        } catch (Exception e) {
            return 0;
        }
    }

    public void sendToDB(String keyword) {

        KafkaProducer<String, String> kafkaProducer = kafkaGeneralConfigurations.getProducer();

        Client twitterClient = twitter.getTwitterClient(msgQueue, keyword);
        twitterClient.connect();

        while (!twitterClient.isDone()) {
            try {
                String rawMessage = msgQueue.poll(10, TimeUnit.SECONDS);
                String messageDTO = convertToDTO(rawMessage);

                if (messageDTO != null) kafkaProducer.send(new ProducerRecord<>("twitter_tweets_db", null, messageDTO));
            } catch (Exception e) {
                log.error(e.getMessage());
                twitterClient.stop();
            }
        }
    }

    private String convertToDTO(final String rawMessage) {
        try {
            JsonObject baseMessage = jsonParser.parse(rawMessage).getAsJsonObject();

            TweetDataDTO tweetDataDTO = TweetDataDTO.builder()
                .userId(baseMessage.get("id_str").getAsString())
                .screenName(baseMessage.get("user").getAsJsonObject().get("screen_name").getAsString())
                .isVerified(baseMessage.get("user").getAsJsonObject().get("verified").getAsBoolean())
                .followersCount(baseMessage.get("user").getAsJsonObject().get("followers_count").getAsInt())
                .statusCount(baseMessage.get("user").getAsJsonObject().get("statuses_count").getAsInt())
                .location(baseMessage.get("user").getAsJsonObject().get("location").toString())
                .build();

            return objectMapper.writeValueAsString(tweetDataDTO);

        } catch (Exception ex) {
            throw new JsonParseException("Deu ruim na conversão do payload do twitter para o DTO", ex);
        }
    }

}
