package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.kafka.Topics;
import com.vandeilson.kafka.service.TwitterService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/twitter")
public class TwitterController {

    @Autowired
    private TwitterService twitterService;

    @GetMapping("/produce/es/{keyword}")
    public void produceTweets(@PathVariable String keyword) {
        twitterService.getRelatedTweets(keyword, Topics.ELASTIC.getTopicName());
    }

    @GetMapping("/produce/db/{keyword}")
    public void produceToDB(@PathVariable String keyword) {
        twitterService.getRelatedTweets(keyword, Topics.DATABASE.getTopicName());
    }

    @GetMapping("/send/es")
    public void sendDataToElasticSearch() {
        twitterService.sendToElasticSearch(Topics.ELASTIC.getTopicName());
    }

    @GetMapping("/send/es/stream")
    public void useKafkaStreams() {
        twitterService.startKafkaStream(Topics.ELASTIC.getTopicName());
    }

    @GetMapping("/send/db")
    public void sendToDB() {
        twitterService.sendToDataBase(Topics.DATABASE.getTopicName());
    }
}
