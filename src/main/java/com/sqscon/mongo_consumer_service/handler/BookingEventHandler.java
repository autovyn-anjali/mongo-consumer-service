package com.sqscon.mongo_consumer_service.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BookingEventHandler {

    public void handle(EventSchema<JsonNode> event) {

        log.info("Processing BOOKING event : {}", event.getEventType());

    }

}