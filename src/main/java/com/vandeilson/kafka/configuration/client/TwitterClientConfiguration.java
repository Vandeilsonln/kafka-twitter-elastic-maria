package com.vandeilson.kafka.configuration.client;


import com.google.common.collect.Lists;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.BasicAuth;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterClientConfiguration {

    @Value("${twitter.consumer.key}")
    String CONSUMER_KEY;

    @Value("${twitter.consumer.secret}")
    String CONSUMER_SECRET;

    @Value("${twitter.token}")
    String TOKEN;

    @Value("${twitter.secret}")
    String SECRET;

    public TwitterClientConfiguration() {}

    public static void main(String[] args) {
        new TwitterClientConfiguration().run();
    }

    public void run() {

    }

    public void createTwitterClient() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);
    }

}
