package com.sqscon.mongo_consumer_service.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.sqscon.mongo_consumer_service.handler.BookingEventHandler;
import com.sqscon.mongo_consumer_service.handler.BuyingEventHandler;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class EventProcessor {

    private final BuyingEventHandler buyingEventHandler;
    private final BookingEventHandler bookingEventHandler;

    public void process(EventSchema<JsonNode> event) {

        Objects.requireNonNull(event, "event must not be null");
        String requestType = event.getRequestType();

        if (requestType == null || requestType.isBlank()) {
            throw new IllegalArgumentException("requestType/module is missing in event: " + event);
        }

        String normalizedRequestType = requestType.trim().toUpperCase(Locale.ROOT);

        switch (normalizedRequestType) {

            case "BUYING":
                buyingEventHandler.handle(event);
                break;

            case "BOOKING":
                bookingEventHandler.handle(event);
                break;

            default:
                throw new IllegalArgumentException(
                        "Unknown request type : " + requestType);
        }

    }

}