package com.vandeilson.kafka.persistence;

import com.vandeilson.kafka.model.entity.TweetData;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TweetDataRepository extends JpaRepository<TweetData, Long> {

}
