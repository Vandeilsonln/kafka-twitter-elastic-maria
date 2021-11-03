package com.vandeilson.kafka.service;

import com.google.gson.JsonParser;
import com.twitter.hbc.core.Client;
import com.vandeilson.kafka.configuration.client.ElasticSearchClientConfiguration;
import com.vandeilson.kafka.configuration.client.TwitterClientConfiguration;
import com.vandeilson.kafka.configuration.kafka.KafkaStreamsConfiguration;
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

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);
    private final JsonParser jsonParser = new JsonParser();

    public void getRelatedTweets(String keyword, KafkaProducer<String, String> producer) {

        Client twitterClient = twitter.getTwitterClient(msgQueue, keyword);
        twitterClient.connect();

        while (!twitterClient.isDone()) {
            String msg;
            try {
                msg = msgQueue.poll(10, TimeUnit.SECONDS);
                log.info(msg);
                if (msg != null) producer.send(new ProducerRecord<>("twitter_tweets", null, msg));
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
            producer.close();
            log.info("Done!");
        }));
    }

    public void sendToElasticSearch(KafkaConsumer<String, String> consumer) {

        RestHighLevelClient esClient = elasticSearch.getClient();

        while(true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                log.info("Received: " + records.count() + " records");
                BulkRequest bulkRequest = fillBulkBatch(records);

                if (records.count() > 0) {
                    esClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    log.info("Commiting Offseets...");
                    consumer.commitSync();
                    log.info("Offsets have been committed.");
                }
            } catch (Exception e) {
                log.error("There was a problem: ");
                log.error(e.getMessage());
            }
        }
    }

    public void startKafkaStream() {
        Properties properties = KafkaStreamsConfiguration.getStreamsProperties();

        // create a topology
        StreamsBuilder builder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = builder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter((k, v) -> extractUserFollowers(v) > 10000);
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
}
