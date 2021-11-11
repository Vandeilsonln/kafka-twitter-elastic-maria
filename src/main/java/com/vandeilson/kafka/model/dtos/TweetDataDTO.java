package com.vandeilson.kafka.model.dtos;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class TweetDataDTO {

    private String userId;
    private String screenName;
    private boolean isVerified;
    private Integer followersCount;
    private Integer statusCount;
    private String location;

}
