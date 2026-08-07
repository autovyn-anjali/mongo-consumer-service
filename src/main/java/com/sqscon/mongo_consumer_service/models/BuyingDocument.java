package com.sqscon.mongo_consumer_service.models;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "buying")
public class BuyingDocument {

    @Id
    private String buyingId;

    private Map<String, Object> data = new HashMap<>();

}