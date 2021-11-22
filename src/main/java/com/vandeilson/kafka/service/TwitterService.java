package com.vandeilson.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.core.Client;
import com.vandeilson.kafka.configuration.client.ElasticSearchClientConfiguration;
import com.vandeilson.kafka.configuration.client.TwitterClientConfiguration;
import com.vandeilson.kafka.configuration.kafka.KafkaGeneralConfigurations;
import com.vandeilson.kafka.model.entity.TweetData;
import com.vandeilson.kafka.persistence.TweetDataRepository;
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

    @Autowired TweetDataRepository tweetDataRepository;

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

    public void startKafkaStream(String topic) {
        Properties properties = kafkaGeneralConfigurations.getStreamsProperties();

        // create a topology
        StreamsBuilder builder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = builder.stream(topic);
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
            } catch (NullPointerException | JsonProcessingException e) {
                log.warn("Skipped bad data: " + i.value());
            }
        }
        return bulkRequest;
    }

    public void sendToDataBase(String topic) {
        KafkaConsumer<String, String> kafkaConsumer = kafkaGeneralConfigurations.getStandardConsumer(topic);
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    String convertedValue = convertToDTO(record.value());
                    TweetData obj = objectMapper.readValue(convertedValue, TweetData.class);
                    tweetDataRepository.save(obj);
                    log.info("Deu certo");
                } catch (JsonProcessingException e) {
                    log.error("Deu ruim :(");
                    e.printStackTrace();
                }
            }
        }

    }

    private String convertToDTO(final String rawMessage) {
        try {
            JsonNode baseMessage = objectMapper.readTree(rawMessage);

            TweetData tweetDataDTO = TweetData.builder()
                .userId(baseMessage.get("id_str").asText())
                .screenName(baseMessage.get("user").get("screen_name").asText())
                .isVerified(baseMessage.get("user").get("verified").asBoolean())
                .followersCount(baseMessage.get("user").get("followers_count").asInt())
                .statusCount(baseMessage.get("user").get("statuses_count").asInt())
                .location(baseMessage.get("user").get("location").asText())
                .build();

            return objectMapper.writeValueAsString(tweetDataDTO);

        } catch (Exception ex) {
            log.error("Ocorreu algum problema na conversão do payload do twitter para o DTO");
            return "";
        }
    }

    private String extractTwitterId(final String tweetJson) throws JsonProcessingException {

        return objectMapper.readTree(tweetJson)
            .get("id_str").asText();
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
