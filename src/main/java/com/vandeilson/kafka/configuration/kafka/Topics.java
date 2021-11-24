package com.vandeilson.kafka.configuration.kafka;

public enum Topics {
    ELASTIC("twitter_tweets"),
    ELASTIC_IMPORTANT("important_tweets"),
    DATABASE("twitter_tweets_db");

    private String topicName;

    Topics(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
