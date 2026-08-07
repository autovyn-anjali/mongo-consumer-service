package com.sqscon.mongo_consumer_service.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.sqscon.mongo_consumer_service.handler.BookingEventHandler;
import com.sqscon.mongo_consumer_service.handler.BuyingEventHandler;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EventProcessor {

    private final BuyingEventHandler buyingEventHandler;
    private final BookingEventHandler bookingEventHandler;

    public void process(EventSchema<JsonNode> event) {

        switch (event.getRequestType()) {

            case "BUYING":
                buyingEventHandler.handle(event);
                break;

            case "BOOKING":
                bookingEventHandler.handle(event);
                break;

            default:
                throw new IllegalArgumentException(
                        "Unknown request type : " + event.getRequestType());
        }

    }

}