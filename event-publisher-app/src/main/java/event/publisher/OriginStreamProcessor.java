package event.publisher;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.DynamoDBStreamsProcessRecordsInput;
import com.amazonaws.services.dynamodbv2.streamsadapter.processor.DynamoDBStreamsShardRecordProcessor;
import event.common.events.EventOriginCodec;
import event.common.events.EventSubmission;
import event.common.events.EventTopics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import tools.jackson.databind.json.JsonMapper;

import static event.common.dynamodb.DynamoDbTableNames.ORIGIN;

@Slf4j
public class OriginStreamProcessor implements DynamoDBStreamsShardRecordProcessor {
    private final DynamoDbClient db;
    private final KafkaTemplate<String, EventSubmission> kafka;
    private final JsonMapper mapper;

    public OriginStreamProcessor(DynamoDbClient db, KafkaTemplate<String, EventSubmission> kafka, JsonMapper mapper) {
        this.db = db;
        this.kafka = kafka;
        this.mapper = mapper;
    }

    @Override
    public void initialize(InitializationInput input) {
        log.info("ORIGIN stream shard assigned: {}", input.shardId());
    }

    @Override
    public void processRecords(DynamoDBStreamsProcessRecordsInput input) {
        for (var streamRecord : input.records()) {
            var record = streamRecord.getRecord();
            if (!"INSERT".equals(record.eventNameAsString()) || record.dynamodb() == null) continue;
            var image = record.dynamodb().newImage();
            if (image == null || !image.containsKey(EventOriginCodec.CLIENT_EVENT_ID)) continue;
            EventSubmission event = EventOriginCodec.decode(image, mapper);
            publishIfActive(event);
        }
        try {
            input.checkpointer().checkpoint();
        } catch (Exception failure) {
            throw new IllegalStateException("ORIGIN stream checkpoint failed", failure);
        }
    }

    void publishIfActive(EventSubmission event) {
        var current = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(EventOriginCodec.key(event.executionId())).consistentRead(true).build()).item();
        if (current.isEmpty() || !EventOriginCodec.STATUS_RECEIVED.equals(current.getOrDefault("status",
                AttributeValue.fromS("")).s()) || current.containsKey("completion_event_id")) return;
        // A stale INSERT must never replace the immutable input with fields from a later update.
        if (!EventOriginCodec.decode(current, mapper).equals(event)) {
            throw new IllegalStateException("ORIGIN changed before Kafka publication: " + event.executionId());
        }
        try {
            kafka.send(EventTopics.HTTP_REQUESTED, event.executionId(), event).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka publication interrupted", interrupted);
        } catch (Exception unconfirmed) {
            throw new IllegalStateException("Kafka publication not confirmed for " + event.executionId(), unconfirmed);
        }
    }

    @Override
    public void leaseLost(LeaseLostInput input) { }

    @Override
    public void shardEnded(ShardEndedInput input) {
        try {
            input.checkpointer().checkpoint();
        } catch (Exception failure) {
            throw new IllegalStateException("ORIGIN stream shard-end checkpoint failed", failure);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput input) {
        try {
            input.checkpointer().checkpoint();
        } catch (Exception failure) {
            throw new IllegalStateException("ORIGIN stream shutdown checkpoint failed", failure);
        }
    }
}
