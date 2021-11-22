package com.vandeilson.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vandeilson.kafka.model.entity.TweetData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JsonConverter {

    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


    public String convertToStringDTO(final String rawMessage) {
        try {
            JsonNode baseMessage = objectMapper.readTree(rawMessage);

            TweetData tweetDataDTO = TweetData.builder()
                .userId(baseMessage.get("id_str").asText())
                .screenName(baseMessage.get("user").get("screen_name").asText())
                .isVerified(baseMessage.get("user").get("verified").asBoolean())
                .followersCount(baseMessage.get("user").get("followers_count").asInt())
                .statusCount(baseMessage.get("user").get("statuses_count").asInt())
                .location(baseMessage.get("user").get("location").asText())
                .build();

            return objectMapper.writeValueAsString(tweetDataDTO);

        } catch (Exception ex) {
            log.error("Ocorreu algum problema na conversão do payload do twitter para o DTO");
            return "";
        }
    }

    public <T> T StringToObject(String payload, Class<T> clazz) {
        T convertedObject = null;
        try {
            convertedObject = objectMapper.readValue(payload, clazz);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return convertedObject;
    }

}
