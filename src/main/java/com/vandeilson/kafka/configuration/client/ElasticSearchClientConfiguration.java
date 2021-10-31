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

    private static final String HOSTNAME = "";
    private static final int PORT = 0;
    private static final String USERNAME = "";
    private static final String PASSWORD = "";

    public static RestHighLevelClient createClient() {

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