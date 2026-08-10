package com.sqscon.mongo_consumer_service.models;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventSchema<T> {

    @JsonAlias("referenceId")
    private String requestId;

    private String eventType;

    @JsonAlias("module")
    private String requestType;

    private T payload;

}
