package com.sqscon.mongo_consumer_service.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import com.sqscon.mongo_consumer_service.service.BuyingMongoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BuyingEventHandler {

    private final BuyingMongoService buyingMongoService;

    public void handle(EventSchema<JsonNode> event) {

        log.info("Processing BUYING event : {}", event.getEventType());
        buyingMongoService.processEvent(event);

    }

}