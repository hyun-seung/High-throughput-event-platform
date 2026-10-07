package messaging.common.messages;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.List;
import java.util.Map;

/** Sparse STEP index for result items that still need a business decision. */
public final class MessageResultInboxIndex {
    public static final String NAME = "message_result_pending_v1";
    public static final String BUCKET = "message_result_bucket";
    public static final String DUE = "message_result_due";
    public static final int SHARDS = 16;

    private MessageResultInboxIndex() { }

    public static void add(Map<String, AttributeValue> item, String clientMsgId, long due) {
        item.put(BUCKET, AttributeValue.fromS(bucket(clientMsgId)));
        item.put(DUE, AttributeValue.fromN(Long.toString(due)));
    }

    public static String bucket(String clientMsgId) {
        return "message-result-v1-" + Math.floorMod(clientMsgId.hashCode(), SHARDS);
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
