package com.vandeilson.kafka.persistence;

import com.vandeilson.kafka.model.entity.TweetData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TweetDataRepository extends JpaRepository<TweetData, Long> {

}
