package messaging.common.messages;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

import java.util.Map;

/** Sparse ORIGIN index: only new admissions carry these attributes. */
public final class MessagePublicationIndex {
    public static final String NAME = "message_publication_due_v1";
    public static final String BUCKET = "message_publication_bucket";
    public static final String DUE = "message_publication_due";
    public static final int SHARDS = 16;

    private MessagePublicationIndex() { }

    public static String bucket(String clientMsgId) {
        return bucketForShard(Math.floorMod(clientMsgId.hashCode(), SHARDS));
    }

    public static String bucketForShard(int shard) {
        if (shard < 0 || shard >= SHARDS) throw new IllegalArgumentException("Invalid publication shard");
        return "message-publication-v1-" + shard;
    }

    public static void add(Map<String, AttributeValue> item, MessageSubmission event) {
        item.put(BUCKET, AttributeValue.fromS(bucket(event.clientMsgId())));
        item.put(DUE, AttributeValue.fromN(Long.toString(event.receivedAt().plusSeconds(60).toEpochMilli())));
    }

    public static GlobalSecondaryIndex definition() {
        return GlobalSecondaryIndex.builder().indexName(NAME)
                .keySchema(KeySchemaElement.builder().attributeName(BUCKET).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(DUE).keyType(KeyType.RANGE).build())
                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()).build();
    }
}
