package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.service.TwitterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/es")
public class ElasticSearchController {

    @Autowired
    TwitterService twitterService;

    @GetMapping
    public void sendDataToElasticSearch() {
        twitterService.sendToElasticSearch("twitter_tweets");
    }
}