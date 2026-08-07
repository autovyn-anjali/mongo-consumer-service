package com.sqscon.mongo_consumer_service.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import com.sqscon.mongo_consumer_service.processor.EventProcessor;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MongoSyncListener {

    private final ObjectMapper objectMapper;
    private final EventProcessor eventProcessor;

    @SqsListener("${aws.sqs.mongo-sync-queue}")
    public void consume(String message) {

        try {

            log.info("Received Message : {}", message);

            EventSchema<JsonNode> event =
                    objectMapper.readValue(
                            message,
                            new TypeReference<EventSchema<JsonNode>>() {});

            eventProcessor.process(event);

        } catch (Exception e) {

            log.error("Error processing SQS message", e);

            throw new RuntimeException(e);
        }

    }

}