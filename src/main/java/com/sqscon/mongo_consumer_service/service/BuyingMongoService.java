package com.sqscon.mongo_consumer_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BuyingMongoService {

    private final MongoTemplate mongoTemplate;
    private final ObjectMapper objectMapper;
    public void processEvent(EventSchema<JsonNode> event) {

        log.info("Mongo update started for buyingId : {}", event.getRequestId());

        String buyingId = event.getRequestId();

        Map<String, Object> payload =
                objectMapper.convertValue(event.getPayload(), Map.class);

        Query query = new Query(
                Criteria.where("_id").is(buyingId)
        );

        Update update = new Update();

        update.set("buyingId", buyingId);

        update.set(
                "data." + event.getEventType(),
                payload
        );

        mongoTemplate.upsert(
                query,
                update,
                "buying"
        );

        log.info(
                "Buying {} updated successfully for {}",
                event.getEventType(),
                buyingId
        );
    }

}