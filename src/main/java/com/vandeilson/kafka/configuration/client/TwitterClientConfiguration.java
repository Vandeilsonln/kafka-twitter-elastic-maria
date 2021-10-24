package com.vandeilson.kafka.configuration.client;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TwitterClientConfiguration {

    private String CONSUMER_KEY = "0tgQQ2ZgZVAh3L2S7q8HaO7Mf";
    private String CONSUMER_SECRET = "xFip1nkqtoOsIUbKdRmXGjfZ4a9N5rFbYJAStfz9gUUwpkhwOw";
    private String TOKEN = "1416196031511994368-H7lQF2rpQsJtnq3WAl3PiD7nDgakll";
    private String SECRET = "RfVkVoZ0ZnjtkMQP4xgBSClz5XVicW0JxIHpttUtGgdJy";

    public TwitterClientConfiguration() {}

    public static void main(String[] args) {
        new TwitterClientConfiguration().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);

        // create a twitter client
        Client client = this.createTwitterClient(msgQueue);

        // Attempts to establish a connection
        client.connect();

        // create a kafka producer

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
                client.stop();
            }

            if (msg != null) {
                log.info(msg);
            }
        }
        log.info("Enf of application!");

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        return new ClientBuilder()
            .name("Twitter-Client-01")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue)).build();

    }

}
