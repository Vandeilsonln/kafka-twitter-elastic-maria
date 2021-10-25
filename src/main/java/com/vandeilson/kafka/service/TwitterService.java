package com.vandeilson.kafka.service;

import com.twitter.hbc.core.Client;
import com.vandeilson.kafka.configuration.client.TwitterClientConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TwitterService {

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);

    public void getRelatedTweets(String keyword) {

        Client twitterClient = TwitterClientConfiguration.getTwitterClient(msgQueue, keyword);
        twitterClient.connect();

        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
                twitterClient.stop();
            }
            if (msg != null) {
                log.info(msg);
            }
        }
    }

}
