package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.client.ElasticSearchClientConfiguration;
import com.vandeilson.kafka.configuration.kafka.Consumers;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.Duration;

@RestController
@RequestMapping("/es")
@Slf4j
public class ElasticSearchController {

    @GetMapping
    public void sendDataToElasticSearch() throws IOException, InterruptedException {
        RestHighLevelClient esClient = ElasticSearchClientConfiguration.getClient();
        KafkaConsumer<String, String> consumer = Consumers.getStandardConsumer("twitter_tweets");

        while(true) {
            try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                String jsonRecord = record.value();

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                    .source(jsonRecord, XContentType.JSON);

                IndexResponse indexResponse = esClient.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                log.info(id);
            }
        } catch (Exception e) {
                log.error("There was a problem");
            }
        }
    }
}