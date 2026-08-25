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

    private static final String COLLECTION_NAME = "buying_master_test1";

    private static final Set<String> DOCUMENT_FIELDS = Set.of(
            "documentId",
            "docId",
            "docUrl",
            "docRemark",
            "docAlias",
            "updatedOn",
            "updatedBy",
            "createdOn",
            "createdBy",
            "systemGenerated",
            "submitted",
            "status",
            "documentStatus",
            "fileKey"
    );

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

        if (!isAllowedEventType(eventType)) {
            log.warn(
                    "Ignoring unsupported eventType={} | buyingId={}",
                    eventType,
                    buyingId
            );
            return;
        }

        if (buyingId == null || buyingId.isBlank()) {
            log.warn(
                    "Ignoring event because buyingId is missing | eventType={}",
                    eventType
            );
            return;
        }

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


        /*
         * =========================================================
         * NORMAL BUYING MASTER FIELDS
         * =========================================================
         */

        Query query = new Query(
                Criteria.where("_id").is(buyingId)
        );

        Update update = new Update();


        Map<String, Object> fieldsToUpdate =
                extractFieldsToUpdate(payload);

        /*
         * =========================================================
         * DOCUMENT EVENT
         * =========================================================
         *
         * Any event containing documentId/docId is treated as
         * a document-related event.
         *
         * We do NOT depend on eventType here.
         *
         * This means:
         *
         * DOCUMENT_REMARK_UPDATE
         * DOCUMENT_IMAGE_NOTIFICATION
         * DOCUMENT_OPERATION_BUYING_UPDATE
         * any future document event
         *
         * can update buyingMaster.documents[]
         */
        if (isDocumentEvent(payload)) {

            processSingleDocumentEvent(
                    buyingId,
                    payload
            );

            /*
             * Remove document-specific fields from the normal
             * buyingMaster update.
             *
             * Otherwise documentId, docRemark, docUrl etc.
             * would also be stored directly inside buyingMaster.
             */
            fieldsToUpdate = new LinkedHashMap<>(fieldsToUpdate);

            fieldsToUpdate.keySet().removeIf(
                    this::isDocumentField
            );
        }


        /*
         * =========================================================
         * NORMAL BUYING MASTER FIELDS
         * =========================================================
         */
        fieldsToUpdate.forEach((key, value) -> {

            if ("documents".equals(key)) {
                return;
            }

            if ("buyingId".equals(key)
                    || "requestType".equals(key)
                    || key.startsWith("$")) {

                return;
            }

            String mongoKey = toSnakeCase(key);

            update.set(
                    "buyingMaster." + mongoKey,
                    value
            );
        });

        /*
         * =========================================================
         * UPDATE NORMAL BUYING MASTER FIELDS
         * =========================================================
         */

        if (!update.getUpdateObject().isEmpty()) {

            UpdateResult result =
                    mongoTemplate.updateFirst(
                            query,
                            update,
                            "buying_master_test1"
                    );

            log.info(
                    "BuyingMaster update | buyingId={} | matched={} | modified={}",
                    buyingId,
                    result.getMatchedCount(),
                    result.getModifiedCount()
            );
        }
    }
    private Map<String, Object> extractFieldsToUpdate(
            Map<String, Object> payload
    ) {

        if (payload.containsKey("$set")) {

            Object setPayload = payload.get("$set");

            if (!(setPayload instanceof Map<?, ?>)) {

                log.warn(
                        "Payload $set is not a map. Ignoring payload={}",
                        payload
                );

                return Map.of();
            }

            return convertToStringObjectMap(setPayload);
        }

        return payload;
    }

    private Map<String, Object> convertToStringObjectMap(
            Object mapLikeObject
    ) {

        Map<?, ?> rawMap =
                objectMapper.convertValue(
                        mapLikeObject,
                        Map.class
                );

        Map<String, Object> normalizedMap =
                new LinkedHashMap<>();

        rawMap.forEach(
                (key, value) ->
                        normalizedMap.put(
                                String.valueOf(key),
                                value
                        )
        );

        return normalizedMap;
    }

    private void updateDocuments(
            String buyingId,
            Object documentsValue
    ) {

        if (!(documentsValue instanceof List<?> documents)) {

            log.warn(
                    "documents field is not a List | buyingId={}",
                    buyingId
            );

            return;
        }

        if (documents.isEmpty()) {
            return;
        }

        for (Object documentValue : documents) {

            if (!(documentValue instanceof Map<?, ?> documentMap)) {

                log.warn(
                        "Invalid document object | buyingId={}",
                        buyingId
                );

                continue;
            }

            Object documentId = documentMap.get("documentId");

            if (documentId == null) {

                log.warn(
                        "documentId missing | buyingId={}",
                        buyingId
                );

                continue;
            }

            /*
             * ---------------------------------------------------------
             * 1. Convert incoming document to Mongo structure
             * ---------------------------------------------------------
             */

            Document mongoDocument = new Document();

            documentMap.forEach((key, value) -> {

                String fieldName = String.valueOf(key);

                String mongoKey = toSnakeCase(fieldName);

                mongoDocument.put(
                        mongoKey,
                        value
                );
            });


            /*
             * ---------------------------------------------------------
             * 2. Try to UPDATE existing document
             * ---------------------------------------------------------
             */

            Query documentQuery = new Query();

            documentQuery.addCriteria(
                    Criteria.where("_id").is(buyingId)
            );

            documentQuery.addCriteria(
                    Criteria.where("buyingMaster.documents.document_id")
                            .is(documentId)
            );


            Update documentUpdate = new Update();


            mongoDocument.forEach((field, value) -> {

                documentUpdate.set(
                        "buyingMaster.documents.$." + field,
                        value
                );

            });


            UpdateResult result = mongoTemplate.updateFirst(
                    documentQuery,
                    documentUpdate,
                    "buying_master_test1"
            );


            /*
             * ---------------------------------------------------------
             * 3. If document didn't exist -> INSERT it
             * ---------------------------------------------------------
             */

            if (result.getMatchedCount() == 0) {

                Query buyingQuery = new Query(
                        Criteria.where("_id").is(buyingId)
                );

                Update pushUpdate = new Update();

                pushUpdate.push(
                        "buyingMaster.documents",
                        mongoDocument
                );

                mongoTemplate.updateFirst(
                        buyingQuery,
                        pushUpdate,
                        "buying_master_test1"
                );


                log.info(
                        "New document added to buyingMaster.documents | buyingId={} | documentId={}",
                        buyingId,
                        documentId
                );

            } else {

                log.info(
                        "Existing document updated in buyingMaster.documents | buyingId={} | documentId={}",
                        buyingId,
                        documentId
                );
            }
        }
    }

    private boolean isAllowedEventType(
            String eventType
    ) {

        if (eventType == null
                || eventType.isBlank()) {

            return false;
        }

        try {

            EventType type =
                    EventType.valueOf(
                            eventType
                                    .trim()
                                    .toUpperCase(Locale.ROOT)
                    );

            return ALLOWED_BUYING_EVENTS.contains(type);

        } catch (IllegalArgumentException e) {

            return false;
        }
    }

    private String toSnakeCase(
            String fieldName
    ) {

        return fieldName
                .replaceAll(
                        "([a-z])([A-Z])",
                        "$1_$2"
                )
                .toLowerCase(Locale.ROOT);
    }

    private boolean isDocumentEvent(
            Map<String, Object> payload
    ) {

        return getDocumentId(payload) != null;
    }

    private Object getDocumentId(
            Map<String, Object> payload
    ) {

        Object documentId = payload.get("documentId");

        if (documentId == null) {
            documentId = payload.get("docId");
        }

        return documentId;
    }

    private boolean isDocumentField(String fieldName) {

        return DOCUMENT_FIELDS.contains(fieldName);
    }

    private void handleSingleDocumentEvent(
            Update update,
            Map<String, Object> payload
    ) {

        Object documentId = getDocumentId(payload);

        if (documentId == null) {

            log.warn(
                    "Document event received without documentId/docId. Payload={}",
                    payload
            );

            return;
        }

        /*
         * ---------------------------------------------------------
         * Build one document object
         * ---------------------------------------------------------
         */

        Document mongoDocument = new Document();

        payload.forEach((key, value) -> {

            if (!isDocumentField(key)) {
                return;
            }

            String mongoKey = toSnakeCase(key);

            /*
             * docId and documentId should be stored
             * consistently as document_id.
             */
            if ("docId".equals(key)) {
                mongoKey = "document_id";
            }

            mongoDocument.put(
                    mongoKey,
                    value
            );
        });

        /*
         * ---------------------------------------------------------
         * Make sure document_id exists
         * ---------------------------------------------------------
         */

        mongoDocument.put(
                "document_id",
                documentId
        );

        /*
         * ---------------------------------------------------------
         * IMPORTANT:
         *
         * We cannot simply push here.
         *
         * If document 14 already exists, push would create:
         *
         * documents: [
         *     {document_id: 14, ...},
         *     {document_id: 14, ...}
         * ]
         *
         * We don't want duplicates.
         * ---------------------------------------------------------
         */

        update.push(
                "buyingMaster.documents",
                mongoDocument
        );

        log.info(
                "Document prepared for Mongo | documentId={} | document={}",
                documentId,
                mongoDocument
        );
    }

    private void processSingleDocumentEvent(
            String buyingId,
            Map<String, Object> payload
    ) {

        Object documentId = getDocumentId(payload);

        if (documentId == null) {

            log.warn(
                    "Document event received without documentId/docId | buyingId={}",
                    buyingId
            );

            return;
        }

        /*
         * ---------------------------------------------------------
         * Build document object
         * ---------------------------------------------------------
         */

        Document document = new Document();

        payload.forEach((key, value) -> {

            if (!isDocumentField(key)) {
                return;
            }

            String mongoKey = toSnakeCase(key);

            if ("docId".equals(key)) {
                mongoKey = "document_id";
            }

            document.put(
                    mongoKey,
                    value
            );
        });

        /*
         * Always keep document_id
         */

        document.put(
                "document_id",
                documentId
        );

        /*
         * ---------------------------------------------------------
         * Find buying document
         * ---------------------------------------------------------
         */

        Query query = new Query(
                Criteria.where("_id").is(buyingId)
        );

        /*
         * ---------------------------------------------------------
         * Check whether this document already exists
         * ---------------------------------------------------------
         */

        Query documentQuery = new Query(
                Criteria.where("_id").is(buyingId)
                        .and("buyingMaster.documents.document_id")
                        .is(documentId)
        );

        boolean documentExists =
                mongoTemplate.exists(
                        documentQuery,
                        COLLECTION_NAME
                );

        /*
         * ---------------------------------------------------------
         * Existing document → UPDATE
         * ---------------------------------------------------------
         */

        if (documentExists) {

            Update update = new Update();

            document.forEach((key, value) -> {

                update.set(
                        "buyingMaster.documents.$[doc]." + key,
                        value
                );
            });

            update.filterArray(
                    Criteria.where("doc.document_id")
                            .is(documentId)
            );

            UpdateResult result =
                    mongoTemplate.updateFirst(
                            query,
                            update,
                            COLLECTION_NAME
                    );

            log.info(
                    "Existing document updated | buyingId={} | documentId={} | matched={} | modified={}",
                    buyingId,
                    documentId,
                    result.getMatchedCount(),
                    result.getModifiedCount()
            );

        } else {

            /*
             * -----------------------------------------------------
             * Document doesn't exist → INSERT into array
             * -----------------------------------------------------
             */

            Update update = new Update();

            update.push(
                    "buyingMaster.documents",
                    document
            );

            UpdateResult result =
                    mongoTemplate.updateFirst(
                            query,
                            update,
                            COLLECTION_NAME
                    );

            log.info(
                    "New document inserted into documents array | buyingId={} | documentId={} | matched={} | modified={}",
                    buyingId,
                    documentId,
                    result.getMatchedCount(),
                    result.getModifiedCount()
            );
        }
    }

}