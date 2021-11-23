package com.vandeilson.kafka.service.implementations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vandeilson.kafka.configuration.client.ElasticSearchClientConfiguration;
import com.vandeilson.kafka.service.interfaces.ConsumeFromKafka;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
public class ConsumeToElasticImpl implements ConsumeFromKafka {

    @Autowired
    ElasticSearchClientConfiguration elasticSearch;

    @Autowired
    ObjectMapper objectMapper;

    @Override
    public void send(final String topic, KafkaConsumer<String, String> kafkaConsumer) {
        RestHighLevelClient esClient = elasticSearch.getClient();

        while(true) {
            try {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                log.info("Received: " + records.count() + " records");
                BulkRequest bulkRequest = fillBulkBatch(records);

                if (records.count() > 0) {
                    esClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    kafkaConsumer.commitSync();
                    log.info("Offsets have been committed.");
                }
            } catch (Exception e) {
                log.error("There was a problem: ");
                log.error(e.getMessage());
            }
        }
    }

    private BulkRequest fillBulkBatch(final ConsumerRecords<String, String> records) {
        BulkRequest bulkRequest = new BulkRequest();

        for (ConsumerRecord<String, String> i : records) {

            // --- Kafka Generic ID ---
            // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
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

    private String extractTwitterId(final String tweetJson) throws JsonProcessingException {

        return objectMapper.readTree(tweetJson)
            .get("id_str").asText();
    }

}
