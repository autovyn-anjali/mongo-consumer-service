package com.sqscon.mongo_consumer_service.repository;

import com.sqscon.mongo_consumer_service.models.BuyingDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface BuyingRepository
        extends MongoRepository<BuyingDocument, String> {
}