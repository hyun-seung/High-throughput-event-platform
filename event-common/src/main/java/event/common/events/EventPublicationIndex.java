package event.common.events;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

import java.util.Map;

/** Sparse ORIGIN index: only new admissions carry these attributes. */
public final class EventPublicationIndex {
    public static final String NAME = "event_publication_due_v1";
    public static final String BUCKET = "event_publication_bucket";
    public static final String DUE = "event_publication_due";
    public static final int SHARDS = 16;

    private EventPublicationIndex() { }

    public static String bucket(String executionId) {
        return "event-publication-v1-" + Math.floorMod(executionId.hashCode(), SHARDS);
    }

    public static void add(Map<String, AttributeValue> item, EventSubmission event) {
        item.put(BUCKET, AttributeValue.fromS(bucket(event.executionId())));
        item.put(DUE, AttributeValue.fromN(Long.toString(event.receivedAt().plusSeconds(60).toEpochMilli())));
    }

    public static GlobalSecondaryIndex definition() {
        return GlobalSecondaryIndex.builder().indexName(NAME)
                .keySchema(KeySchemaElement.builder().attributeName(BUCKET).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(DUE).keyType(KeyType.RANGE).build())
                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()).build();
    }
}
