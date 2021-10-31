package com.vandeilson.kafka.configuration.client;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchClientConfiguration {

    private ElasticSearchClientConfiguration() {}

    // Replace with your own credentials
    private static final String HOSTNAME = "kafka-course-705596321.us-east-1.bonsaisearch.net";
    private static final int PORT = 443;
    private static final String USERNAME = "ial8hxadec";
    private static final String PASSWORD = "hzyncr12p6";

    public static RestHighLevelClient getClient() {

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