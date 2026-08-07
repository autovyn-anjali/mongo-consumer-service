package com.sqscon.mongo_consumer_service.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventSchema<T> {

    private String requestId;

    private String eventType;

    private String requestType;

    private T payload;

}
