package com.vandeilson.kafka.controller;

import com.vandeilson.kafka.configuration.client.TwitterClientConfiguration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/twitter")
public class TwitterController {

    @GetMapping
    public void getTweets() {
        var a = new TwitterClientConfiguration();
        a.run();
    }
}
