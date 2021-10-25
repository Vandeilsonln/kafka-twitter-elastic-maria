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
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;

@Slf4j
@NoArgsConstructor
public class TwitterClientConfiguration {

    private static final String CONSUMER_KEY = "0tgQQ2ZgZVAh3L2S7q8HaO7Mf";
    private static final String CONSUMER_SECRET = "xFip1nkqtoOsIUbKdRmXGjfZ4a9N5rFbYJAStfz9gUUwpkhwOw";
    private static final String TOKEN = "1416196031511994368-H7lQF2rpQsJtnq3WAl3PiD7nDgakll";
    private static final String SECRET = "RfVkVoZ0ZnjtkMQP4xgBSClz5XVicW0JxIHpttUtGgdJy";

    public static Client getTwitterClient(BlockingQueue<String> msgQueue, String element) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList(element);
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        return new ClientBuilder()
            .name("Twitter-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue)).build();

    }

}
