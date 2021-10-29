package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.kafka.Producers;
import com.vandeilson.kafka.service.TwitterService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/twitter")
@Slf4j
public class TwitterController {

    @Autowired
    private TwitterService twitterService;

    @GetMapping("/{keyword}")
    public void produceTweets(@PathVariable String keyword) {

        twitterService.sendRelatedTweets(keyword, Producers.getProducer());

    }

}
