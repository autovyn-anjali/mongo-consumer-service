package com.sqscon.mongo_consumer_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.result.UpdateResult;
import com.sqscon.mongo_consumer_service.enums.EventType;
import com.sqscon.mongo_consumer_service.models.EventSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class BuyingMongoService {

    private final MongoTemplate mongoTemplate;
    private final ObjectMapper objectMapper;


    private static final Set<EventType> ALLOWED_BUYING_EVENTS =
            EnumSet.of(
                    EventType.BUYING_CREATED,
                    EventType.FASTAG_BUYING_UPDATED,
                    EventType.CHALLAN_COUNT_UPDATED,
                    EventType.BUYING_BYPASS_UPDATED,
                    EventType.BUYING_BYPASS_AUDIT_CREATED,
                    EventType.BOOKING_BYPASS_UPDATED,
                    EventType.UPDATE_DETAILS,
                    EventType.GEO_TAGGING_BUYING_UPDATED,
                    EventType.ADDRESS_UPDATE,
                    EventType.AD_BUYING_ADDRESS_UPDATED,
                    EventType.SALES_DOCKET_UPDATED,
                    EventType.AD_BOOKING_ADDRESS_UPDATED,
                    EventType.AD_SET_NAME_FINAL_SIMILARITY,
                    EventType.AADHAR_INITIATED,
                    EventType.BOOKING_ADDRESS_UPDATED,
                    EventType.CUSTOMER_DOCKET_UPDATED,
                    EventType.DEALER_DOCKET_UPDATED,
                    EventType.PRESIGNED_URL_GENERATION,
                    EventType.DOCUMENT_IMAGE_NOTIFICATION,
                    EventType.DOCUMENT_REMARK_UPDATE,
                    EventType.DOCUMENT_OPERATION_BUYING_UPDATE,
                    EventType.DOCUMENT_OPERATION_BOOKING_UPDATE,
                    EventType.CUSTOMER_DOCKET_UPDATE,
                    EventType.DEALER_DOCKET_UPDATE,
                    EventType.SALES_DOCKET_UPDATE,
                    EventType.API_HIT_LOG

            );

    public void processEvent(EventSchema<JsonNode> event) {

        if (event == null) {
            log.warn("Ignoring null event");
            return;
        }

        String buyingId = event.getRequestId();
        String eventType = event.getEventType();

        /*
         * ---------------------------------------------------------
         * 1. Validate event type
         * ---------------------------------------------------------
         */

        if (!isAllowedEventType(eventType)) {

            log.warn(
                    "Ignoring unsupported eventType={} | buyingId={}",
                    eventType,
                    buyingId
            );

            return;
        }

        /*
         * ---------------------------------------------------------
         * 2. Validate buyingId
         * ---------------------------------------------------------
         */

        if (buyingId == null || buyingId.isBlank()) {

            log.warn(
                    "Ignoring event because buyingId is missing | eventType={}",
                    eventType
            );

            return;
        }

        log.info(
                "Mongo update started | buyingId={} | eventType={}",
                buyingId,
                eventType
        );



        Map<String, Object> payload =
                objectMapper.convertValue(
                        event.getPayload(),
                        Map.class
                );

        if (payload == null || payload.isEmpty()) {

            log.warn(
                    "Empty payload received | buyingId={} | eventType={}",
                    buyingId,
                    eventType
            );

            return;
        }



        Query query = new Query(
                Criteria.where("buying_id").is(buyingId)
        );



        Update update = new Update();


        Map<String, Object> fieldsToUpdate = extractFieldsToUpdate(payload);

        if (fieldsToUpdate.isEmpty()) {
            log.warn(
                    "No fields available after payload normalization | buyingId={} | eventType={}",
                    buyingId,
                    eventType
            );
            return;
        }

        fieldsToUpdate.forEach((key, value) -> {


            if ("buyingId".equals(key)
                    || "requestType".equals(key)
                    || key.startsWith("$")) {

                return;
            }

            if ("documents".equals(key)) {

                handleDocuments(update, value);

                return;
            }


            String mongoKey = toSnakeCase(key);

            update.set(mongoKey, value);

            log.debug(
                    "Mongo field prepared | buyingId={} | field={} | value={}",
                    buyingId,
                    mongoKey,
                    value
            );
        });



        if (update.getUpdateObject().isEmpty()) {

            log.warn(
                    "No fields available for Mongo update | buyingId={} | eventType={}",
                    buyingId,
                    eventType
            );

            return;
        }

        log.info("Mongo query: {}", query);
        log.info("Mongo update: {}", update);



        UpdateResult result = mongoTemplate.updateFirst(
                query,
                update,
                "buying"
        );

        log.info(
                "Mongo update result | buyingId={} | eventType={} | matched={} | modified={} | upsertedId={}",
                buyingId,
                eventType,
                result.getMatchedCount(),
                result.getModifiedCount(),
                result.getUpsertedId()
        );



        if (result.getMatchedCount() == 0) {

            log.warn(
                    "No Mongo document found | buyingId={} | eventType={}",
                    buyingId,
                    eventType
            );

            return;
        }

        log.info(
                "Buying Mongo update successful | buyingId={} | eventType={}",
                buyingId,
                eventType
        );
    }

    private Map<String, Object> extractFieldsToUpdate(
            Map<String, Object> payload
    ) {

        if (payload.containsKey("$set")) {

            Object setPayload = payload.get("$set");

            if (!(setPayload instanceof Map<?, ?>)) {
                log.warn("Payload $set is not a map. Ignoring payload={}", payload);
                return Map.of();
            }

            return convertToStringObjectMap(setPayload);
        }

        return payload;
    }

    private Map<String, Object> convertToStringObjectMap(
            Object mapLikeObject
    ) {

        Map<?, ?> rawMap = objectMapper.convertValue(
                mapLikeObject,
                Map.class
        );

        Map<String, Object> normalizedMap = new LinkedHashMap<>();

        rawMap.forEach((key, value) ->
                normalizedMap.put(String.valueOf(key), value)
        );

        return normalizedMap;
    }


    private void handleDocuments(
            Update update,
            Object documentsValue
    ) {

        if (!(documentsValue instanceof List<?> documents)) {

            log.warn(
                    "documents field is not a List. Ignoring documents update."
            );

            return;
        }

        if (documents.isEmpty()) {
            return;
        }


        for (Object documentValue : documents) {

            if (!(documentValue instanceof Map<?, ?> documentMap)) {

                log.warn(
                        "Invalid document object inside documents array. Ignoring."
                );

                continue;
            }

            Object documentId = documentMap.get("documentId");

            if (documentId == null) {

                log.warn(
                        "documentId is missing. Ignoring document object={}",
                        documentMap
                );

                continue;
            }

            Document mongoDocument = new Document();

            documentMap.forEach((key, value) -> {

                String fieldName = String.valueOf(key);

                String mongoKey = toSnakeCase(fieldName);

                mongoDocument.put(
                        mongoKey,
                        value
                );
            });

            update.push("documents", mongoDocument);
        }
    }

    private boolean isAllowedEventType(String eventType) {

        if (eventType == null || eventType.isBlank()) {
            return false;
        }

        try {

            EventType type = EventType.valueOf(
                    eventType.trim().toUpperCase(Locale.ROOT)
            );

            return ALLOWED_BUYING_EVENTS.contains(type);

        } catch (IllegalArgumentException e) {

            return false;
        }
    }


    private String toSnakeCase(String fieldName) {

        return fieldName
                .replaceAll(
                        "([a-z])([A-Z])",
                        "$1_$2"
                )
                .toLowerCase(Locale.ROOT);
    }
}
