package com.vandeilson.kafka.service;

import com.google.gson.JsonParser;
import com.twitter.hbc.core.Client;
import com.vandeilson.kafka.configuration.client.ElasticSearchClientConfiguration;
import com.vandeilson.kafka.configuration.client.TwitterClientConfiguration;
import com.vandeilson.kafka.configuration.kafka.Consumers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TwitterService {

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);
    private final JsonParser jsonParser = new JsonParser();

    public void sendRelatedTweets(String keyword, KafkaProducer<String, String> producer) {

        Client twitterClient = TwitterClientConfiguration.getTwitterClient(msgQueue, keyword);
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

    public void sendToElasticSearch(KafkaConsumer<String, String> consumer) throws InterruptedException {

        RestHighLevelClient esClient = ElasticSearchClientConfiguration.getClient();

        while(true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                log.info("Received: " + records.count() + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> i : records) {

                    // --- Kafka Generic ID ---
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // --- Twitter Feed Id ---
                    try {
                        String twitterId = extractTwitterId(i.value());

                        String jsonRecord = i.value();
                        IndexRequest indexRequest = new IndexRequest("twitter", "tweets", twitterId)
                            .source(jsonRecord, XContentType.JSON);

                        bulkRequest.add(indexRequest);
                    } catch (NullPointerException e) {
                        log.warn("Skipped bad data: " + i.value());
                    }
                }
                if (records.count() > 0) {
                    BulkResponse bulkItemResponses = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Commiting Offseets...");
                    Thread.sleep(1000);
                    consumer.commitSync();
                    log.info("Offsets have been committed.");
                }

            } catch (Exception e) {
                log.error("There was a problem");
                log.error(e.getMessage());
            }

        }
    }

    private String extractTwitterId(final String tweetJson) {
        // gson library
        return jsonParser.parse(tweetJson)
            .getAsJsonObject()
            .get("id_str")
        .getAsString();

    }

}
