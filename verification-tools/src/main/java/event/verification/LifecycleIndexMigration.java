package event.verification;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/** Explicit local-only lifecycle GSI migration. Never scans; only listed delivery IDs are backfilled. */
final class LifecycleIndexMigration {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String INDEX = "lifecycle_due_v1";
    private static final List<String> TABLES = List.of("ORIGIN", "STEP");

    private LifecycleIndexMigration() {}

    static void run(String[] args) throws Exception {
        String endpoint = "http://localhost:18000";
        boolean apply = false;
        var ids = new ArrayList<String>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--endpoint" -> endpoint = args[++i];
                case "--apply" -> apply = true;
                case "--delivery-id" -> ids.add(args[++i]);
                default -> throw new IllegalArgumentException("Usage: lifecycle-index [--endpoint http://localhost:18000] [--apply] [--delivery-id UUID]...");
            }
        }
        URI uri = localEndpoint(endpoint);
        try (var db = DynamoDbClient.builder().endpointOverride(uri).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(10))).build()) {
            migrate(db, apply, ids, System.out::println);
        }
    }

    static URI localEndpoint(String value) {
        URI uri = URI.create(value);
        if (!"http".equals(uri.getScheme()) || !List.of("localhost", "127.0.0.1").contains(uri.getHost()))
            throw new IllegalArgumentException("Local DynamoDB only");
        return uri;
    }

    static void migrate(DynamoDbClient db, boolean apply, List<String> ids, Consumer<String> output) throws Exception {
        for (String table : TABLES) {
            boolean exists = hasIndex(db, table, false);
            var report = new LinkedHashMap<String, Object>();
            report.put("table", table);
            report.put("indexExists", exists);
            report.put("apply", apply);
            report.put("backfillIds", ids);
            output.accept(JSON.writeValueAsString(report));
            if (!apply) continue;
            if (!exists) {
                db.updateTable(UpdateTableRequest.builder().tableName(table)
                        .attributeDefinitions(
                                AttributeDefinition.builder().attributeName("lifecycle_bucket").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("lifecycle_due").attributeType(ScalarAttributeType.N).build())
                        .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                                .create(CreateGlobalSecondaryIndexAction.builder().indexName(INDEX)
                                        .keySchema(KeySchemaElement.builder().attributeName("lifecycle_bucket").keyType("HASH").build(),
                                                KeySchemaElement.builder().attributeName("lifecycle_due").keyType("RANGE").build())
                                        .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()).build()).build())
                        .build());
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (!hasIndex(db, table, true)) {
                if (System.nanoTime() >= deadline)
                    throw new IllegalStateException("Index not ACTIVE; retain existing state and retry later");
                Thread.sleep(500);
            }
            for (String delivery : ids) {
                canonicalId(delivery);
                var items = new ArrayList<Map<String, AttributeValue>>();
                Map<String, AttributeValue> cursor = Map.of();
                do {
                    var builder = QueryRequest.builder().tableName(table).keyConditionExpression("pk = :pk")
                            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("DELIVERY#" + delivery)))
                            .consistentRead(true);
                    if (!cursor.isEmpty()) builder.exclusiveStartKey(cursor);
                    var page = db.query(builder.build());
                    items.addAll(page.items());
                    cursor = page.lastEvaluatedKey();
                } while (cursor != null && !cursor.isEmpty());
                boolean hasAttempt = items.stream().anyMatch(item -> value(item, "sk").startsWith("ATTEMPT#"));
                for (var item : items) {
                    var update = plan(table, delivery, item, hasAttempt);
                    if (update != null) db.updateItem(update);
                }
                output.accept("backfilled " + delivery);
            }
        }
    }

    private static boolean hasIndex(DynamoDbClient db, String table, boolean active) {
        var indexes = db.describeTable(request -> request.tableName(table)).table().globalSecondaryIndexes();
        return indexes.stream().anyMatch(index -> INDEX.equals(index.indexName())
                && (!active || "ACTIVE".equals(index.indexStatusAsString())));
    }

    static UpdateItemRequest plan(String table, String delivery, Map<String, AttributeValue> item, boolean hasAttempt) {
        if (item.containsKey("lifecycle_bucket") || item.containsKey("completion_event_id")) return null;
        if (Boolean.TRUE.equals(item.getOrDefault("lifecycle_closed", AttributeValue.fromBool(false)).bool())
                && !"PENDING".equals(value(item, "publish_state"))) return null;
        String sk = value(item, "sk");
        String condition = "attribute_exists(pk) AND attribute_not_exists(lifecycle_bucket)";
        var names = new LinkedHashMap<String, String>();
        var values = new LinkedHashMap<String, AttributeValue>();
        values.put(":bucket", AttributeValue.fromS(bucket(delivery)));
        long due;
        if (sk.equals("META") && !hasAttempt) due = 0;
        else if (sk.startsWith("ATTEMPT#") && "PENDING".equals(value(item, "publish_state"))) {
            due = 0;
            condition += " AND publish_state = :pending AND result_event = :event AND #version = :version";
            names.put("#version", "version");
            values.put(":pending", AttributeValue.fromS("PENDING"));
            values.put(":event", item.get("result_event"));
            values.put(":version", item.get("version"));
        } else if (sk.startsWith("ATTEMPT#")) {
            String state = value(item, "status");
            if (!List.of("DELIVERED", "DECISION_PENDING", "RETRY_SCHEDULED", "PROCESSING", "ACCEPTED", "REVIEW_REQUIRED").contains(state))
                throw new IllegalArgumentException("Unrecognized Attempt state; manual review required");
            due = List.of("DELIVERED", "DECISION_PENDING").contains(state) ? 0 : Long.parseLong(number(item));
            condition += " AND #version = :version AND #state = :state AND attribute_not_exists(lifecycle_closed)";
            names.put("#version", "version");
            names.put("#state", "status");
            values.put(":version", item.get("version"));
            values.put(":state", item.get("status"));
        } else if (sk.equals("FINAL") && "PENDING".equals(value(item, "publish_state"))) {
            due = 0;
            condition += " AND publish_state = :pending AND result_event = :event";
            values.put(":pending", AttributeValue.fromS("PENDING"));
            values.put(":event", item.get("result_event"));
        } else return null;
        values.put(":due", AttributeValue.fromN(Long.toString(due)));
        var builder = UpdateItemRequest.builder().tableName(table)
                .key(Map.of("pk", item.get("pk"), "sk", item.get("sk")))
                .updateExpression("SET lifecycle_bucket = :bucket, lifecycle_due = :due")
                .conditionExpression(condition).expressionAttributeValues(values);
        if (!names.isEmpty()) builder.expressionAttributeNames(names);
        return builder.build();
    }

    private static String number(Map<String, AttributeValue> item) {
        for (String name : List.of("next_attempt_at", "deadline_at", "primary_deadline"))
            if (item.containsKey(name)) return item.get(name).n();
        throw new IllegalArgumentException("Attempt deadline absent");
    }

    private static String value(Map<String, AttributeValue> item, String key) {
        var value = item.get(key);
        return value == null ? "" : value.s();
    }

    static String bucket(String delivery) {
        if (!delivery.chars().allMatch(character -> character <= 127))
            throw new IllegalArgumentException("Expected ASCII delivery ID");
        return "lifecycle-v1-" + Math.floorMod(delivery.hashCode(), 16);
    }

    private static void canonicalId(String delivery) {
        if (!UUID.fromString(delivery).toString().equals(delivery))
            throw new IllegalArgumentException("Expected canonical UUID");
    }
}
