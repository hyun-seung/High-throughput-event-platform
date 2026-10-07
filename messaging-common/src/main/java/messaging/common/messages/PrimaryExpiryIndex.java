package messaging.common.messages;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Sparse ORIGIN index for the 3-hour first-send result deadline. */
public final class PrimaryExpiryIndex {
    public static final String NAME = "message_primary_expiry_v1";
    public static final String BUCKET = "message_primary_expiry_bucket";
    public static final String DUE = "message_primary_expiry_due";
    public static final String DEADLINE = "message_primary_deadline_at_ms";
    public static final int SHARDS = 16;
    public static final Duration TTL = Duration.ofHours(3);

    private PrimaryExpiryIndex() { }

    public static String bucket(String clientMsgId) {
        return "message-primary-expiry-v1-" + Math.floorMod(clientMsgId.hashCode(), SHARDS);
    }

    public static void add(Map<String, AttributeValue> origin, MessageSubmission admission) {
        long deadline = admission.receivedAt().plus(TTL).toEpochMilli();
        origin.put(BUCKET, AttributeValue.fromS(bucket(admission.clientMsgId())));
        origin.put(DUE, AttributeValue.fromN(Long.toString(deadline)));
        origin.put(DEADLINE, AttributeValue.fromN(Long.toString(deadline)));
    }

    public static List<AttributeDefinition> attributes() {
        return List.of(
                AttributeDefinition.builder().attributeName(BUCKET).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(DUE).attributeType(ScalarAttributeType.N).build());
    }

    public static GlobalSecondaryIndex definition() {
        return GlobalSecondaryIndex.builder().indexName(NAME)
                .keySchema(KeySchemaElement.builder().attributeName(BUCKET).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(DUE).keyType(KeyType.RANGE).build())
                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()).build();
    }
}
