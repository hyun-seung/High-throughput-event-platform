package event.common.lifecycle;

import software.amazon.awssdk.services.dynamodb.model.*;
import java.util.Map;

/** Persisted schema: changing shard count/names requires an explicit migration. */
public final class LifecycleIndex {
    public static final String NAME = "lifecycle_due_v1", BUCKET = "lifecycle_bucket", DUE = "lifecycle_due";
    public static final int SHARDS = 16;
    private LifecycleIndex() { }
    public static String bucket(String deliveryId) { return bucket(Math.floorMod(deliveryId.hashCode(), SHARDS)); }
    public static String bucket(int shard) { return "lifecycle-v1-" + shard; }
    public static void add(Map<String, AttributeValue> item, String deliveryId, long due) {
        item.put(BUCKET, AttributeValue.fromS(bucket(deliveryId)));
        item.put(DUE, AttributeValue.fromN(Long.toString(due)));
    }
    public static GlobalSecondaryIndex definition() {
        return GlobalSecondaryIndex.builder().indexName(NAME)
                .keySchema(KeySchemaElement.builder().attributeName(BUCKET).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(DUE).keyType(KeyType.RANGE).build())
                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()).build();
    }
}
