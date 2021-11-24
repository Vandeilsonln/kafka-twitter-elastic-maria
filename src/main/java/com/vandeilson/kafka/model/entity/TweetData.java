package com.vandeilson.kafka.model.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@AllArgsConstructor
@Builder
@Entity
@Getter
@Setter
public class TweetData {

    public TweetData() {
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String userId;
    private String screenName;
    private boolean isVerified;
    private Integer followersCount;
    private Integer statusCount;
    private String location;

}
