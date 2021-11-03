package com.vandeilson.kafka.configuration.client;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ElasticSearchClientConfiguration {

    private ElasticSearchClientConfiguration() {}

    @Value("${elastic.hostname}")
    private String HOSTNAME = "kafka-course-705596321.us-east-1.bonsaisearch.net";

    @Value("${elastic.port}")
    private int PORT = 443;

    @Value("${elastic.username}")
    private String USERNAME = "ial8hxadec";

    @Value("${elastic.password}")
    private String PASSWORD = "hzyncr12p6";

    public RestHighLevelClient getClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(USERNAME, PASSWORD));

        RestClientBuilder builder = RestClient.builder(
            new HttpHost(HOSTNAME, PORT, "https"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

}