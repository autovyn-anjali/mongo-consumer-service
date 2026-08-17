package com.sqscon.mongo_consumer_service.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import com.sqscon.mongo_consumer_service.service.BookingMongoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BookingEventHandler {

    private final BookingMongoService bookingMongoService;

    public void handle(EventSchema<JsonNode> event) {

        log.info("Processing BOOKING event : {}", event.getEventType());
        bookingMongoService.processEvent(event);

    }

}