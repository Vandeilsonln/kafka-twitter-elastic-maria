package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.client.ElasticSearchClientConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/es")
@Slf4j
public class ElasticSearchController {

    @GetMapping
    public void createElastic() throws IOException {
        RestHighLevelClient esClient = ElasticSearchClientConfiguration.getClient();

        String exampleJson = "{ \"foo\": \"bar\" }";

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
            .source(exampleJson, XContentType.JSON);

        IndexResponse indexResponse = esClient.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();

        log.info(id);

        esClient.close();
    }

}
