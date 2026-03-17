package com.dataplatform.ingestion.producer;

import com.dataplatform.ingestion.model.TradeEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * TradeEventProducer
 *
 * Publishes trade lifecycle events to Kafka with:
 * - Idempotent exactly-once semantics (enable.idempotence=true)
 * - Schema-validated Avro payloads (see application.yml for schema registry)
 * - Partition key = instrumentId for ordered processing per instrument
 * - Dead-letter topic routing on serialisation failure
 *
 * Topic naming convention: dp.{domain}.{entity}.{version}
 * Example:            dp.trading.trade-events.v1
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TradeEventProducer {

    private static final String TOPIC = "dp.trading.trade-events.v1";
    private static final String DLT_TOPIC = "dp.trading.trade-events.v1.dlt";

    private final KafkaTemplate<String, TradeEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Publish a trade event. Partition key is instrumentId to guarantee
     * ordering for all events on the same instrument.
     */
    public CompletableFuture<RecordMetadata> publish(TradeEvent event) {
        validateEvent(event);

        ProducerRecord<String, TradeEvent> record = new ProducerRecord<>(
            TOPIC,
            event.getInstrumentId(),   // partition key
            event
        );

        // Attach correlation headers for distributed tracing
        record.headers().add("correlationId", event.getCorrelationId().getBytes());
        record.headers().add("sourceSystem", event.getSourceSystem().getBytes());
        record.headers().add("schemaVersion", "v1".getBytes());

        CompletableFuture<SendResult<String, TradeEvent>> future =
            kafkaTemplate.send(record);

        return future.handle((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish trade event tradeId={} correlationId={}",
                    event.getTradeId(), event.getCorrelationId(), ex);
                routeToDlt(event, ex.getMessage());
                throw new RuntimeException("Publish failed", ex);
            }
            RecordMetadata meta = result.getRecordMetadata();
            log.info("Published tradeId={} topic={} partition={} offset={}",
                event.getTradeId(), meta.topic(), meta.partition(), meta.offset());
            return meta;
        });
    }

    private void validateEvent(TradeEvent event) {
        // TODO: integrate with JSON Schema / Avro schema registry validation
        if (event.getTradeId() == null || event.getInstrumentId() == null) {
            throw new IllegalArgumentException("tradeId and instrumentId are mandatory");
        }
    }

    private void routeToDlt(TradeEvent event, String errorMessage) {
        // TODO: enrich with error metadata before DLT publish
        kafkaTemplate.send(DLT_TOPIC, event.getTradeId(), event);
        log.warn("Routed to DLT tradeId={} reason={}", event.getTradeId(), errorMessage);
    }
}
