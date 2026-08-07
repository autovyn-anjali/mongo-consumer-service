package com.sqscon.mongo_consumer_service.repository;

import com.sqscon.mongo_consumer_service.models.BookingDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface BookingRepository
        extends MongoRepository<BookingDocument, String> {
}