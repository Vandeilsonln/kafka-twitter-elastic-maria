package com.vandeilson.kafka.service.implementations;

import com.vandeilson.kafka.model.entity.TweetData;
import com.vandeilson.kafka.persistence.TweetDataRepository;
import com.vandeilson.kafka.service.interfaces.ConsumeFromKafka;
import com.vandeilson.kafka.utils.JsonConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
public class ConsumeToDataBaseImpl implements ConsumeFromKafka {

    @Autowired TweetDataRepository tweetDataRepository;
    @Autowired JsonConverter jsonConverter;

    @Override
    public void send(final String topic, KafkaConsumer<String, String> kafkaConsumer) {
        while(true) {
            try {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                log.info("Received: " + records.count() + " records");

                for (ConsumerRecord<String, String> myRecord : records) {
                    TweetData tweetObject = jsonConverter.StringToObject(jsonConverter.convertToStringDTO(myRecord.value()), TweetData.class);
                    tweetDataRepository.save(tweetObject);
                    log.info("Deu certo");
                }

                kafkaConsumer.commitSync();
                log.info("Offsets have been committed.");
            } catch (Exception ex) {
                ex.printStackTrace();
                log.error("Ocorreu algum problema");
            }

        }

    }

}
