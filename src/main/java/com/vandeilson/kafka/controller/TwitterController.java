package com.vandeilson.kafka.controller;

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

    @GetMapping("/{keyword}")
    public void produceTweets(@PathVariable String keyword) {
        twitterService.getRelatedTweets(keyword);
    }

    @GetMapping("/stream")
    public void useKafkaStreams() {
        twitterService.startKafkaStream();
    }
}
